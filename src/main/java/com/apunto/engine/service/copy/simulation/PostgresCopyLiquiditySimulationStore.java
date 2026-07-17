package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.LiquiditySimulationAssumptions;
import com.apunto.copytarget.LiquiditySimulationResult;
import com.apunto.copytarget.OrderBookSnapshot;
import com.apunto.copytarget.SourceSide;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class PostgresCopyLiquiditySimulationStore implements CopyLiquiditySimulationStore {

    private static final String CLAIM_SQL = """
            WITH candidates AS (
                SELECT id
                FROM copy_liquidity_job_v3
                WHERE status IN ('PENDING', 'NO_BOOK')
                  AND next_run_at <= now()
                ORDER BY next_run_at, created_at, id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE copy_liquidity_job_v3 job
            SET status = 'RUNNING', locked_at = now(), locked_by = ?, updated_at = now()
            FROM candidates
            WHERE job.id = candidates.id
            RETURNING job.id, job.capital_scenario_id, job.symbol,
                      job.position_side, job.requested_notional_usd, job.attempt
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Value("${copy.simulation.liquidity-worker.max-attempts:5}")
    private int maxAttempts = 5;

    @Override
    @Transactional
    public List<CopyLiquidityCandidate> claimBatch(String workerId, int limit) {
        if (limit <= 0) return List.of();
        return jdbcTemplate.query(CLAIM_SQL, this::mapCandidate, limit,
                workerId == null || workerId.isBlank() ? "cold-liquidity" : workerId);
    }

    @Override
    public int requeueStale(OffsetDateTime threshold) {
        return jdbcTemplate.update("""
                UPDATE copy_liquidity_job_v3
                SET status = 'PENDING', locked_at = NULL, locked_by = NULL, updated_at = now()
                WHERE status = 'RUNNING' AND locked_at < ?
                """, threshold);
    }

    @Override
    @Transactional
    public void saveResults(CopyLiquidityCandidate candidate,
                            OrderBookSnapshot snapshot,
                            LiquiditySimulationAssumptions assumptions,
                            List<LiquiditySimulationResult> results) {
        UUID snapshotId = persistSnapshot(snapshot);
        String assumptionsJson = writeJson(assumptions);
        for (LiquiditySimulationResult result : results) {
            if (result.realValidated()) {
                throw new IllegalArgumentException("simulated liquidity cannot be REAL_VALIDATED");
            }
            jdbcTemplate.update("""
                    INSERT INTO copy_liquidity_simulation_v3 (
                        liquidity_job_id, capital_scenario_id, order_book_snapshot_id, execution_strategy,
                        status, requested_notional_usd, filled_notional_usd, vwap, expected_slippage_bps,
                        depth_consumed_pct, fill_percentage, unfilled_notional_usd,
                        estimated_execution_ms, market_participation_pct, estimated_fees_usd,
                        estimated_funding_usd, adverse_selection_bps,
                        source_closed_before_completion, evidence_level, model_version,
                        assumptions, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), now())
                    ON CONFLICT (liquidity_job_id, execution_strategy, order_book_snapshot_id)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        requested_notional_usd = EXCLUDED.requested_notional_usd,
                        filled_notional_usd = EXCLUDED.filled_notional_usd,
                        vwap = EXCLUDED.vwap,
                        expected_slippage_bps = EXCLUDED.expected_slippage_bps,
                        depth_consumed_pct = EXCLUDED.depth_consumed_pct,
                        fill_percentage = EXCLUDED.fill_percentage,
                        unfilled_notional_usd = EXCLUDED.unfilled_notional_usd,
                        estimated_execution_ms = EXCLUDED.estimated_execution_ms,
                        market_participation_pct = EXCLUDED.market_participation_pct,
                        estimated_fees_usd = EXCLUDED.estimated_fees_usd,
                        estimated_funding_usd = EXCLUDED.estimated_funding_usd,
                        adverse_selection_bps = EXCLUDED.adverse_selection_bps,
                        source_closed_before_completion = EXCLUDED.source_closed_before_completion,
                        evidence_level = EXCLUDED.evidence_level,
                        model_version = EXCLUDED.model_version,
                        assumptions = EXCLUDED.assumptions
                    """,
                    candidate.id(), candidate.capitalScenarioId(), snapshotId, result.executionStrategy().name(),
                    result.status().name(), result.requestedNotionalUsd(), result.filledNotionalUsd(), result.vwap(),
                    result.expectedSlippageBps(), result.depthConsumedPct(), result.fillPercentage(),
                    result.unfilledNotionalUsd(), result.estimatedExecutionMillis(),
                    result.marketParticipationPct(), result.estimatedFeesUsd(), result.estimatedFundingUsd(),
                    result.adverseSelectionBps(), result.sourceClosedBeforeCompletion(),
                    result.evidenceLevel().name(), result.modelVersion(), assumptionsJson);
        }
        jdbcTemplate.update("""
                UPDATE copy_liquidity_job_v3
                SET status = 'COMPLETED', locked_at = NULL, locked_by = NULL,
                    last_error = NULL, updated_at = now()
                WHERE id = ? AND status = 'RUNNING'
                """, candidate.id());
    }

    @Override
    public void markNoBook(CopyLiquidityCandidate candidate, String reason, OffsetDateTime retryAt) {
        finishRetryable(candidate, "NO_BOOK", reason, retryAt);
    }

    @Override
    public void markFailed(CopyLiquidityCandidate candidate, String reason, OffsetDateTime retryAt) {
        finishRetryable(candidate, "PENDING", reason, retryAt);
    }

    private void finishRetryable(CopyLiquidityCandidate candidate,
                                 String retryStatus,
                                 String reason,
                                 OffsetDateTime retryAt) {
        jdbcTemplate.update("""
                UPDATE copy_liquidity_job_v3
                SET attempt = attempt + 1,
                    status = CASE WHEN attempt + 1 >= ? THEN 'FAILED' ELSE ? END,
                    next_run_at = ?, locked_at = NULL, locked_by = NULL,
                    last_error = ?, updated_at = now()
                WHERE id = ? AND status = 'RUNNING'
                """, Math.max(1, maxAttempts), retryStatus, retryAt, reason, candidate.id());
    }

    private UUID persistSnapshot(OrderBookSnapshot snapshot) {
        UUID id = UUID.randomUUID();
        int inserted = jdbcTemplate.update("""
                INSERT INTO copy_order_book_snapshot_v3 (
                    id, symbol, exchange, captured_at, source, sequence_number,
                    bids, asks, model_version, created_at
                ) VALUES (?, ?, 'BINANCE', ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), 'liquidity-v3', now())
                ON CONFLICT (exchange, symbol, captured_at, model_version) DO NOTHING
                """, id, snapshot.symbol(), snapshot.capturedAt(), snapshot.source(), snapshot.sequenceNumber(),
                writeJson(snapshot.bids()), writeJson(snapshot.asks()));
        if (inserted == 1) return id;
        UUID existing = jdbcTemplate.queryForObject("""
                SELECT id FROM copy_order_book_snapshot_v3
                WHERE exchange = 'BINANCE' AND symbol = ? AND captured_at = ? AND model_version = 'liquidity-v3'
                """, UUID.class, snapshot.symbol(), snapshot.capturedAt());
        if (existing == null) throw new IllegalStateException("order book snapshot conflict was not found");
        return existing;
    }

    private CopyLiquidityCandidate mapCandidate(ResultSet resultSet, int rowNumber) throws SQLException {
        return new CopyLiquidityCandidate(
                resultSet.getObject("id", UUID.class),
                resultSet.getLong("capital_scenario_id"),
                resultSet.getString("symbol"),
                SourceSide.valueOf(resultSet.getString("position_side")),
                resultSet.getBigDecimal("requested_notional_usd"),
                resultSet.getInt("attempt")
        );
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("liquidity payload is not serializable", ex);
        }
    }
}
