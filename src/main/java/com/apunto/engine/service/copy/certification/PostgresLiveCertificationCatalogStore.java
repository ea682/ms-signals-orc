package com.apunto.engine.service.copy.certification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class PostgresLiveCertificationCatalogStore implements LiveCertificationCatalogStore {

    private static final String SELECT_COLUMNS = """
            SELECT id, wallet_id, strategy_code, strategy_version, scope_type, scope_value,
                   capital_band_min, capital_band_max, target_leverage, exchange, quote_asset,
                   sizing_policy_version, symbol_mapping_version, fee_model_version,
                   funding_model_version, slippage_model_version, liquidity_model_version,
                   evidence_level, certification_status, version
            FROM strategy_live_certification
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public Optional<LiveCertificationCatalogRecord> findByCreationKey(String creationKey) {
        return jdbcTemplate.query(SELECT_COLUMNS + " WHERE creation_key = ?", this::map, creationKey)
                .stream().findFirst();
    }

    @Override
    public Optional<LiveCertificationCatalogRecord> findByIdentity(LiveCertificationIdentity identity) {
        return exact(identity).stream().findFirst();
    }

    @Override
    public boolean insert(LiveCertificationCatalogRecord record, String creationKey,
                          Map<String, Object> evidenceSnapshot, String actor, String reason) {
        LiveCertificationIdentity identity = record.identity();
        return jdbcTemplate.update("""
                        INSERT INTO strategy_live_certification (
                            id, creation_key, wallet_id, strategy_code, strategy_version,
                            scope_type, scope_value, capital_band_min, capital_band_max,
                            target_leverage, exchange, quote_asset, sizing_policy_version,
                            symbol_mapping_version, fee_model_version, funding_model_version,
                            slippage_model_version, liquidity_model_version, evidence_level,
                            certification_status, evidence_snapshot, automatic_promotion_enabled,
                            version, created_by, creation_reason, created_at, updated_at
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            CAST(? AS jsonb), FALSE, 0, ?, ?, now(), now()
                        )
                        ON CONFLICT DO NOTHING
                        """,
                record.id(), creationKey, identity.walletId(), identity.strategyCode(),
                identity.strategyVersion(), identity.scopeType(), identity.scopeValue(),
                identity.capitalBandMin(), identity.capitalBandMax(), identity.targetLeverage(),
                identity.exchange(), identity.quoteAsset(), identity.sizingPolicyVersion(),
                identity.symbolMappingVersion(), identity.feeModelVersion(),
                identity.fundingModelVersion(), identity.slippageModelVersion(),
                identity.liquidityModelVersion(), record.evidenceLevel().name(), record.status().name(),
                writeJson(evidenceSnapshot), actor, reason) == 1;
    }

    @Override
    public Optional<LiveCertificationIdentity> findIdentityById(UUID certificationId) {
        return jdbcTemplate.query(SELECT_COLUMNS + " WHERE id = ?", this::map, certificationId)
                .stream().findFirst().map(LiveCertificationCatalogRecord::identity);
    }

    private List<LiveCertificationCatalogRecord> exact(LiveCertificationIdentity identity) {
        return jdbcTemplate.query(SELECT_COLUMNS + """
                        WHERE wallet_id = ? AND strategy_code = ? AND strategy_version = ?
                          AND scope_type = ? AND scope_value = ?
                          AND capital_band_min = ? AND capital_band_max = ?
                          AND target_leverage = ? AND exchange = ? AND quote_asset = ?
                          AND sizing_policy_version = ? AND symbol_mapping_version = ?
                          AND fee_model_version = ? AND funding_model_version = ?
                          AND slippage_model_version = ? AND liquidity_model_version = ?
                        """, this::map,
                identity.walletId(), identity.strategyCode(), identity.strategyVersion(),
                identity.scopeType(), identity.scopeValue(), identity.capitalBandMin(),
                identity.capitalBandMax(), identity.targetLeverage(), identity.exchange(),
                identity.quoteAsset(), identity.sizingPolicyVersion(), identity.symbolMappingVersion(),
                identity.feeModelVersion(), identity.fundingModelVersion(),
                identity.slippageModelVersion(), identity.liquidityModelVersion());
    }

    private LiveCertificationCatalogRecord map(ResultSet rs, int rowNumber) throws SQLException {
        LiveCertificationIdentity identity = new LiveCertificationIdentity(
                rs.getString("wallet_id"), rs.getString("strategy_code"),
                rs.getString("strategy_version"), rs.getString("scope_type"), rs.getString("scope_value"),
                rs.getBigDecimal("capital_band_min"), rs.getBigDecimal("capital_band_max"),
                rs.getBigDecimal("target_leverage"), rs.getString("exchange"), rs.getString("quote_asset"),
                rs.getString("sizing_policy_version"), rs.getString("symbol_mapping_version"),
                rs.getString("fee_model_version"), rs.getString("funding_model_version"),
                rs.getString("slippage_model_version"), rs.getString("liquidity_model_version"));
        return new LiveCertificationCatalogRecord(
                rs.getObject("id", UUID.class), identity,
                LiveEvidenceLevel.valueOf(rs.getString("evidence_level")),
                LiveCertificationStatus.valueOf(rs.getString("certification_status")),
                rs.getLong("version"));
    }

    private String writeJson(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Certification evidence is not serializable", ex);
        }
    }
}
