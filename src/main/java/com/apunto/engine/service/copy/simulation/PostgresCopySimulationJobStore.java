package com.apunto.engine.service.copy.simulation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class PostgresCopySimulationJobStore implements CopySimulationJobStore {

    private static final String INSERT_JOB_SQL = """
            INSERT INTO copy_simulation_job_v3 (
                id, idempotency_key, input_hash, source_event_id, source_snapshot_version,
                allocation_id, user_id, wallet_id, strategy_code, strategy_version,
                scope_type, scope_value, sizing_policy_version, symbol_mapping_version,
                execution_mode, input_snapshot, status, resume_cursor, pause_requested,
                attempt, next_run_at, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb),
                'PENDING', 0, FALSE, 0, now(), now(), now()
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            """;

    private static final String CLAIM_SQL = """
            WITH candidates AS (
                SELECT id
                FROM copy_simulation_job_v3
                WHERE status = 'PENDING'
                  AND pause_requested = FALSE
                  AND next_run_at <= now()
                ORDER BY next_run_at, created_at, id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE copy_simulation_job_v3 job
            SET status = 'RUNNING', locked_at = now(), locked_by = ?, updated_at = now()
            FROM candidates
            WHERE job.id = candidates.id
            RETURNING job.id, job.source_event_id, job.allocation_id,
                      job.resume_cursor, job.input_snapshot::text
            """;

    private static final String UPSERT_SCENARIO_SQL = """
            INSERT INTO copy_capital_leverage_simulation_v3 (
                job_id, scenario_index, capital_usd, target_leverage,
                target_notional_usd, target_margin_usd, positions_copied,
                positions_omitted, movement_coverage, notional_coverage,
                exposure_coverage, rounding_loss_usd, min_notional_skips,
                fees_usd, funding_usd, slippage_usd, gross_pnl_usd, net_pnl_usd,
                drawdown_pct, profit_factor, liquidation_risk,
                modeled_economics_status, target_portfolio, simulation_only,
                created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                CAST(? AS jsonb), TRUE, now(), now()
            )
            ON CONFLICT (job_id, scenario_index) DO UPDATE SET
                capital_usd = EXCLUDED.capital_usd,
                target_leverage = EXCLUDED.target_leverage,
                target_notional_usd = EXCLUDED.target_notional_usd,
                target_margin_usd = EXCLUDED.target_margin_usd,
                positions_copied = EXCLUDED.positions_copied,
                positions_omitted = EXCLUDED.positions_omitted,
                movement_coverage = EXCLUDED.movement_coverage,
                notional_coverage = EXCLUDED.notional_coverage,
                exposure_coverage = EXCLUDED.exposure_coverage,
                rounding_loss_usd = EXCLUDED.rounding_loss_usd,
                min_notional_skips = EXCLUDED.min_notional_skips,
                fees_usd = EXCLUDED.fees_usd,
                funding_usd = EXCLUDED.funding_usd,
                slippage_usd = EXCLUDED.slippage_usd,
                gross_pnl_usd = EXCLUDED.gross_pnl_usd,
                net_pnl_usd = EXCLUDED.net_pnl_usd,
                drawdown_pct = EXCLUDED.drawdown_pct,
                profit_factor = EXCLUDED.profit_factor,
                liquidation_risk = EXCLUDED.liquidation_risk,
                modeled_economics_status = EXCLUDED.modeled_economics_status,
                target_portfolio = EXCLUDED.target_portfolio,
                simulation_only = TRUE,
                updated_at = now()
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Value("${copy.simulation.worker.max-attempts:5}")
    private int maxAttempts = 5;

    @Value("${copy.simulation.liquidity-worker.minimum-capital-usd:5000}")
    private java.math.BigDecimal liquidityMinimumCapitalUsd = new java.math.BigDecimal("5000");

    @Override
    @Transactional
    public boolean enqueue(CopySimulationContext context, CopySimulationInputSnapshot snapshot) {
        String json = writeJson(snapshot);
        String inputHash = sha256(json);
        String idempotencyKey = sha256(identityMaterial(context, snapshot));
        int inserted = jdbcTemplate.update(
                INSERT_JOB_SQL,
                UUID.randomUUID(),
                idempotencyKey,
                inputHash,
                context.sourceEventId(),
                snapshot.sourceSnapshotVersion(),
                context.allocationId(),
                context.userId(),
                context.walletId(),
                context.strategyCode(),
                context.strategyVersion(),
                context.scopeType(),
                context.scopeValue(),
                snapshot.versions().sizingPolicyVersion(),
                snapshot.versions().symbolMappingVersion(),
                context.executionMode(),
                json
        );
        if (inserted == 1) {
            return true;
        }
        String existingHash = jdbcTemplate.queryForObject(
                "SELECT input_hash FROM copy_simulation_job_v3 WHERE idempotency_key = ?",
                String.class,
                idempotencyKey
        );
        if (!Objects.equals(inputHash, existingHash)) {
            throw new CopySimulationPayloadConflictException(
                    "simulation identity was reused with a different input snapshot"
                            + " previousHash=" + existingHash + " incomingHash=" + inputHash);
        }
        return false;
    }

    @Override
    @Transactional
    public List<CopySimulationJob> claimBatch(String workerId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        return jdbcTemplate.query(
                CLAIM_SQL,
                this::mapJob,
                limit,
                workerId == null || workerId.isBlank() ? "cold-simulation" : workerId
        );
    }

    @Override
    public boolean isPauseRequested(UUID jobId) {
        Boolean paused = jdbcTemplate.queryForObject(
                "SELECT pause_requested FROM copy_simulation_job_v3 WHERE id = ?",
                Boolean.class,
                jobId
        );
        return Boolean.TRUE.equals(paused);
    }

    @Override
    @Transactional
    public void saveScenario(UUID jobId, CopySimulationScenarioFact scenario) {
        jdbcTemplate.update(
                UPSERT_SCENARIO_SQL,
                jobId,
                scenario.scenarioIndex(),
                scenario.capitalUsd(),
                scenario.targetLeverage(),
                scenario.targetNotionalUsd(),
                scenario.targetMarginUsd(),
                scenario.positionsCopied(),
                scenario.positionsOmitted(),
                scenario.movementCoverage(),
                scenario.notionalCoverage(),
                scenario.exposureCoverage(),
                scenario.roundingLossUsd(),
                scenario.minNotionalSkips(),
                scenario.feesUsd(),
                scenario.fundingUsd(),
                scenario.slippageUsd(),
                scenario.grossPnlUsd(),
                scenario.netPnlUsd(),
                scenario.drawdownPct(),
                scenario.profitFactor(),
                scenario.liquidationRisk(),
                scenario.modeledEconomicsStatus(),
                writeJson(scenario.targetPortfolio())
        );
        jdbcTemplate.update("""
                UPDATE copy_simulation_job_v3
                SET resume_cursor = GREATEST(resume_cursor, ?), updated_at = now()
                WHERE id = ? AND status = 'RUNNING'
                """, scenario.scenarioIndex() + 1, jobId);
        enqueueLiquidityTasks(jobId, scenario);
    }

    @Override
    public void markCompleted(UUID jobId) {
        jdbcTemplate.update("""
                UPDATE copy_simulation_job_v3
                SET status = 'COMPLETED', resume_cursor = 40, locked_at = NULL,
                    locked_by = NULL, completed_at = now(), updated_at = now()
                WHERE id = ? AND status = 'RUNNING'
                """, jobId);
    }

    @Override
    public void markPaused(UUID jobId) {
        jdbcTemplate.update("""
                UPDATE copy_simulation_job_v3
                SET status = 'PAUSED', locked_at = NULL, locked_by = NULL, updated_at = now()
                WHERE id = ? AND status = 'RUNNING'
                """, jobId);
    }

    @Override
    public void markFailed(UUID jobId, String error, OffsetDateTime retryAt) {
        jdbcTemplate.update("""
                UPDATE copy_simulation_job_v3
                SET attempt = attempt + 1,
                    status = CASE WHEN attempt + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END,
                    next_run_at = ?, locked_at = NULL, locked_by = NULL,
                    last_error = ?, updated_at = now()
                WHERE id = ? AND status = 'RUNNING'
                """, Math.max(1, maxAttempts), retryAt, error, jobId);
    }

    @Override
    public int requeueStale(OffsetDateTime threshold) {
        return jdbcTemplate.update("""
                UPDATE copy_simulation_job_v3
                SET status = CASE WHEN pause_requested THEN 'PAUSED' ELSE 'PENDING' END,
                    locked_at = NULL, locked_by = NULL, updated_at = now()
                WHERE status = 'RUNNING' AND locked_at < ?
                """, threshold);
    }

    @Override
    public boolean requestPause(UUID jobId) {
        return jdbcTemplate.update("""
                UPDATE copy_simulation_job_v3
                SET pause_requested = TRUE,
                    status = CASE WHEN status = 'PENDING' THEN 'PAUSED' ELSE status END,
                    updated_at = now()
                WHERE id = ? AND status IN ('PENDING', 'RUNNING')
                """, jobId) == 1;
    }

    @Override
    public boolean resume(UUID jobId) {
        return jdbcTemplate.update("""
                UPDATE copy_simulation_job_v3
                SET pause_requested = FALSE, status = 'PENDING', next_run_at = now(),
                    locked_at = NULL, locked_by = NULL, last_error = NULL, updated_at = now()
                WHERE id = ? AND status IN ('PAUSED', 'FAILED') AND resume_cursor < 40
                """, jobId) == 1;
    }

    private CopySimulationJob mapJob(ResultSet resultSet, int rowNumber) throws SQLException {
        String json = resultSet.getString("input_snapshot");
        try {
            return new CopySimulationJob(
                    resultSet.getObject("id", UUID.class),
                    resultSet.getString("source_event_id"),
                    resultSet.getObject("allocation_id", Long.class),
                    resultSet.getInt("resume_cursor"),
                    objectMapper.readValue(json, CopySimulationInputSnapshot.class)
            );
        } catch (JsonProcessingException ex) {
            throw new SQLException("invalid copy simulation input snapshot", ex);
        }
    }

    private void enqueueLiquidityTasks(UUID jobId, CopySimulationScenarioFact scenario) {
        if (scenario.capitalUsd().compareTo(liquidityMinimumCapitalUsd) < 0
                || scenario.targetPortfolio() == null) {
            return;
        }
        Long capitalScenarioId = jdbcTemplate.queryForObject("""
                SELECT id FROM copy_capital_leverage_simulation_v3
                WHERE job_id = ? AND scenario_index = ?
                """, Long.class, jobId, scenario.scenarioIndex());
        if (capitalScenarioId == null) {
            throw new IllegalStateException("persisted capital scenario was not found");
        }
        Map<LiquidityLegKey, java.math.BigDecimal> grouped = new LinkedHashMap<>();
        scenario.targetPortfolio().selectedLegs().forEach(leg -> {
            LiquidityLegKey key = new LiquidityLegKey(leg.targetSymbol(), leg.side().name());
            grouped.merge(key, leg.targetNotionalUsd(), java.math.BigDecimal::add);
        });
        grouped.forEach((key, notional) -> {
            if (notional == null || notional.signum() <= 0) return;
            jdbcTemplate.update("""
                    INSERT INTO copy_liquidity_job_v3 (
                        id, capital_scenario_id, symbol, position_side,
                        requested_notional_usd, status, attempt, next_run_at,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'PENDING', 0, now(), now(), now())
                    ON CONFLICT (capital_scenario_id, symbol, position_side) DO UPDATE SET
                        requested_notional_usd = EXCLUDED.requested_notional_usd,
                        status = CASE
                            WHEN copy_liquidity_job_v3.status IN ('PENDING', 'NO_BOOK', 'FAILED')
                                THEN 'PENDING'
                            ELSE copy_liquidity_job_v3.status
                        END,
                        next_run_at = CASE
                            WHEN copy_liquidity_job_v3.status IN ('PENDING', 'NO_BOOK', 'FAILED')
                                THEN now()
                            ELSE copy_liquidity_job_v3.next_run_at
                        END,
                        updated_at = now()
                    """, UUID.randomUUID(), capitalScenarioId, key.symbol(), key.side(), notional);
        });
    }

    private String identityMaterial(CopySimulationContext context, CopySimulationInputSnapshot snapshot) {
        return String.join("|",
                context.sourceEventId(),
                String.valueOf(context.allocationId()),
                context.walletId(),
                context.strategyCode(),
                context.strategyVersion(),
                context.scopeType(),
                context.scopeValue(),
                String.valueOf(snapshot.sourceSnapshotVersion()),
                snapshot.versions().sizingPolicyVersion(),
                snapshot.versions().symbolMappingVersion());
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("copy simulation payload is not serializable", ex);
        }
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable", ex);
        }
    }

    private record LiquidityLegKey(String symbol, String side) {
    }
}
