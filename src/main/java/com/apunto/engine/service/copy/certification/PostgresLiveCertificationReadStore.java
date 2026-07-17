package com.apunto.engine.service.copy.certification;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class PostgresLiveCertificationReadStore implements LiveCertificationReadStore {

    private static final String FIND_SQL = """
            SELECT c.id AS certification_id,
                   c.wallet_id, c.strategy_code, c.strategy_version,
                   c.scope_type, c.scope_value,
                   c.capital_band_min, c.capital_band_max, c.target_leverage,
                   c.exchange, c.quote_asset, c.sizing_policy_version,
                   c.symbol_mapping_version, c.fee_model_version,
                   c.funding_model_version, c.slippage_model_version,
                   c.liquidity_model_version, c.certification_status,
                   a.id AS adoption_id, a.validation_status AS adoption_status,
                   COALESCE(a.balance_valid, FALSE) AS balance_valid,
                   COALESCE(a.capital_band_valid, FALSE) AS capital_band_valid,
                   COALESCE(a.leverage_valid, FALSE) AS leverage_valid,
                   COALESCE(a.quote_asset_valid, FALSE) AS quote_asset_valid,
                   COALESCE(a.margin_mode_valid, FALSE) AS margin_mode_valid,
                   COALESCE(a.api_permissions_valid, FALSE) AS api_permissions_valid,
                   COALESCE(a.manual_positions_valid, FALSE) AS manual_positions_valid,
                   COALESCE(a.risk_policy_valid, FALSE) AS risk_policy_valid,
                   a.validated_at, a.expires_at
            FROM strategy_live_certification c
            LEFT JOIN user_live_certification_adoption a
              ON a.certification_id = c.id
             AND a.user_id = ?
             AND a.allocation_id = ?
            WHERE lower(c.wallet_id) = ?
              AND c.strategy_code = ?
              AND c.strategy_version = ?
              AND c.scope_type = ?
              AND c.scope_value = ?
              AND ? BETWEEN c.capital_band_min AND c.capital_band_max
              AND c.target_leverage = ?
              AND c.exchange = ?
              AND c.quote_asset = ?
              AND c.sizing_policy_version = ?
              AND c.symbol_mapping_version = ?
              AND c.fee_model_version = ?
              AND c.funding_model_version = ?
              AND c.slippage_model_version = ?
              AND c.liquidity_model_version = ?
            ORDER BY c.id
            """;

    private final JdbcTemplate jdbcTemplate;

    @Override
    public List<LiveCertificationAuthorizationRecord> findCandidates(LiveEntryAuthorizationRequest request) {
        return jdbcTemplate.query(FIND_SQL, this::map,
                request.userId(), request.allocationId(), request.walletId(), request.strategyCode(),
                request.strategyVersion(), request.scopeType(), request.scopeValue(),
                request.allocatedCapitalUsd(), request.targetLeverage(), request.exchange(),
                request.quoteAsset(), request.sizingPolicyVersion(), request.symbolMappingVersion(),
                request.feeModelVersion(), request.fundingModelVersion(),
                request.slippageModelVersion(), request.liquidityModelVersion());
    }

    private LiveCertificationAuthorizationRecord map(ResultSet rs, int rowNumber) throws SQLException {
        LiveCertificationIdentity identity = new LiveCertificationIdentity(
                rs.getString("wallet_id"), rs.getString("strategy_code"),
                rs.getString("strategy_version"), rs.getString("scope_type"),
                rs.getString("scope_value"), rs.getBigDecimal("capital_band_min"),
                rs.getBigDecimal("capital_band_max"), rs.getBigDecimal("target_leverage"),
                rs.getString("exchange"), rs.getString("quote_asset"),
                rs.getString("sizing_policy_version"), rs.getString("symbol_mapping_version"),
                rs.getString("fee_model_version"), rs.getString("funding_model_version"),
                rs.getString("slippage_model_version"), rs.getString("liquidity_model_version"));
        return new LiveCertificationAuthorizationRecord(
                rs.getObject("certification_id", UUID.class), identity,
                LiveCertificationStatus.valueOf(rs.getString("certification_status")),
                rs.getObject("adoption_id", UUID.class), rs.getString("adoption_status"),
                rs.getBoolean("balance_valid"), rs.getBoolean("capital_band_valid"),
                rs.getBoolean("leverage_valid"), rs.getBoolean("quote_asset_valid"),
                rs.getBoolean("margin_mode_valid"), rs.getBoolean("api_permissions_valid"),
                rs.getBoolean("manual_positions_valid"), rs.getBoolean("risk_policy_valid"),
                rs.getObject("validated_at", java.time.OffsetDateTime.class),
                rs.getObject("expires_at", java.time.OffsetDateTime.class));
    }
}
