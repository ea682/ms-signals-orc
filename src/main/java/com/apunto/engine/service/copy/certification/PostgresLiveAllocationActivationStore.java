package com.apunto.engine.service.copy.certification;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class PostgresLiveAllocationActivationStore implements LiveAllocationActivationStore {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public Optional<LiveAllocationActivationSnapshot> lockAllocation(Long allocationId) {
        return jdbcTemplate.query("""
                        SELECT id, id_user, wallet_id, copy_strategy_code, scope_type, scope_value,
                               execution_mode, status, is_active, ends_at
                        FROM futuros_operaciones.user_copy_allocation
                        WHERE id = ?
                        FOR UPDATE
                        """,
                (rs, rowNum) -> new LiveAllocationActivationSnapshot(
                        rs.getLong("id"), rs.getObject("id_user", UUID.class),
                        rs.getString("wallet_id"), rs.getString("copy_strategy_code"),
                        rs.getString("scope_type"), rs.getString("scope_value"),
                        rs.getString("execution_mode"), rs.getString("status"),
                        rs.getBoolean("is_active"), rs.getObject("ends_at", OffsetDateTime.class)),
                allocationId).stream().findFirst();
    }

    @Override
    public Optional<LiveAllocationActivationAudit> findAudit(String activationKey) {
        return jdbcTemplate.query("""
                        SELECT activation_key, allocation_id, certification_id, user_id,
                               prior_mode, next_mode, actor, reason, occurred_at
                        FROM live_allocation_activation_audit
                        WHERE activation_key = ?
                        """,
                (rs, rowNum) -> new LiveAllocationActivationAudit(
                        rs.getString("activation_key"), rs.getLong("allocation_id"),
                        rs.getObject("certification_id", UUID.class),
                        rs.getObject("user_id", UUID.class), rs.getString("prior_mode"),
                        rs.getString("next_mode"), rs.getString("actor"), rs.getString("reason"),
                        rs.getObject("occurred_at", OffsetDateTime.class)),
                activationKey).stream().findFirst();
    }

    @Override
    public long countOpenOperations(Long allocationId) {
        Long count = jdbcTemplate.queryForObject("""
                SELECT count(*)
                FROM futuros_operaciones.copy_operation
                WHERE user_copy_allocation_id = ? AND is_active = TRUE
                """, Long.class, allocationId);
        return count == null ? 0L : count;
    }

    @Override
    public long countNonTerminalIntents(Long allocationId) {
        Long count = jdbcTemplate.queryForObject("""
                SELECT count(*)
                FROM futuros_operaciones.copy_dispatch_intent
                WHERE user_copy_allocation_id = ?
                  AND (
                      status NOT IN ('PERSISTED', 'REJECTED', 'FAILED_FINAL', 'CANCELLED', 'DUPLICATE')
                      OR reservation_status = 'RESERVED'
                  )
                """, Long.class, allocationId);
        return count == null ? 0L : count;
    }

    @Override
    public Optional<LiveActivationAuthorization> findAuthorization(Long allocationId, UUID certificationId) {
        return jdbcTemplate.query("""
                        SELECT c.id AS certification_id, c.certification_status,
                               a.validation_status AS adoption_status,
                               (
                                   lower(c.wallet_id) = lower(u.wallet_id)
                                   AND c.strategy_code = u.copy_strategy_code
                                   AND c.scope_type = u.scope_type
                                   AND c.scope_value = u.scope_value
                                   AND a.balance_valid AND a.capital_band_valid
                                   AND a.leverage_valid AND a.quote_asset_valid
                                   AND a.margin_mode_valid AND a.api_permissions_valid
                                   AND a.manual_positions_valid AND a.risk_policy_valid
                                   AND a.assigned_capital_usd BETWEEN c.capital_band_min AND c.capital_band_max
                                   AND a.target_leverage = c.target_leverage
                                   AND a.quote_asset = c.quote_asset
                               ) AS all_checks_valid,
                               a.validated_at, a.expires_at
                        FROM futuros_operaciones.user_copy_allocation u
                        JOIN strategy_live_certification c ON c.id = ?
                        JOIN user_live_certification_adoption a
                          ON a.certification_id = c.id
                         AND a.user_id = u.id_user
                         AND a.allocation_id = u.id
                        WHERE u.id = ?
                          AND (u.live_certification_id IS NULL OR u.live_certification_id = c.id)
                        """,
                (rs, rowNum) -> new LiveActivationAuthorization(
                        rs.getObject("certification_id", UUID.class),
                        LiveCertificationStatus.valueOf(rs.getString("certification_status")),
                        rs.getString("adoption_status"), rs.getBoolean("all_checks_valid"),
                        rs.getObject("validated_at", OffsetDateTime.class),
                        rs.getObject("expires_at", OffsetDateTime.class)),
                certificationId, allocationId).stream().findFirst();
    }

    @Override
    public boolean activate(Long allocationId, String actor, String reason, OffsetDateTime activatedAt) {
        return jdbcTemplate.update("""
                        UPDATE futuros_operaciones.user_copy_allocation
                        SET execution_mode = 'LIVE',
                            status_reason = 'MANUAL_LIVE_CERTIFICATION_ACTIVATED',
                            status_updated_at = ?, updated_at = ?
                        WHERE id = ?
                          AND execution_mode = 'MICRO_LIVE'
                          AND is_active = TRUE
                          AND ends_at IS NULL
                          AND lower(status) = 'active'
                        """, activatedAt, activatedAt, allocationId) == 1;
    }

    @Override
    public boolean activatePendingLive(Long allocationId, String actor, String reason,
                                       OffsetDateTime activatedAt) {
        jdbcTemplate.update("""
                UPDATE futuros_operaciones.user_copy_allocation pending_micro
                SET status = 'CLOSED', is_active = FALSE, ends_at = ?,
                    status_reason = 'RECERTIFIED_LIVE_ACTIVATED', status_updated_at = ?, updated_at = ?
                FROM futuros_operaciones.user_copy_allocation pending_live
                WHERE pending_live.id = ?
                  AND pending_micro.id_user = pending_live.id_user
                  AND lower(pending_micro.wallet_id) = lower(pending_live.wallet_id)
                  AND pending_micro.copy_strategy_code = pending_live.copy_strategy_code
                  AND pending_micro.scope_type = pending_live.scope_type
                  AND pending_micro.scope_value = pending_live.scope_value
                  AND pending_micro.execution_mode = 'MICRO_LIVE'
                  AND pending_micro.ends_at IS NULL
                """, activatedAt, activatedAt, activatedAt, allocationId);
        return jdbcTemplate.update("""
                        UPDATE futuros_operaciones.user_copy_allocation
                        SET status = 'ACTIVE',
                            status_reason = 'LIVE_CERTIFICATION_ADOPTION_ACTIVATED',
                            status_updated_at = ?, updated_at = ?, activation_at = ?
                        WHERE id = ?
                          AND execution_mode = 'LIVE'
                          AND is_active = TRUE
                          AND ends_at IS NULL
                          AND lower(status) = 'paused'
                        """, activatedAt, activatedAt, activatedAt, allocationId) == 1;
    }

    @Override
    public void appendAudit(LiveAllocationActivationAudit audit) {
        jdbcTemplate.update("""
                        INSERT INTO live_allocation_activation_audit (
                            id, activation_key, allocation_id, certification_id, user_id,
                            prior_mode, next_mode, actor, reason, occurred_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                UUID.randomUUID(), audit.activationKey(), audit.allocationId(),
                audit.certificationId(), audit.userId(), audit.priorMode(), audit.nextMode(),
                audit.actor(), audit.reason(), audit.occurredAt());
    }
}
