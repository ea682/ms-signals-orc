package com.apunto.engine.service.copy.certification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class PostgresLiveUserAdoptionStore implements LiveUserAdoptionStore {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void upsert(UserAdoptionValidationRequest request, UserAdoptionValidationDecision decision) {
        jdbcTemplate.update("""
                        INSERT INTO user_live_certification_adoption (
                            id, certification_id, user_id, allocation_id, validation_status,
                            balance_usd, assigned_capital_usd, target_leverage, quote_asset,
                            observed_margin_mode, required_margin_mode, balance_valid,
                            capital_band_valid, leverage_valid, quote_asset_valid,
                            margin_mode_valid, api_permissions_valid, manual_positions_valid,
                            risk_policy_valid, reason_codes, observed_at, validated_at,
                            expires_at, created_at, updated_at
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            CAST(? AS jsonb), ?, now(), ?, now(), now()
                        )
                        ON CONFLICT (certification_id, user_id, allocation_id) DO UPDATE SET
                            validation_status = EXCLUDED.validation_status,
                            balance_usd = EXCLUDED.balance_usd,
                            assigned_capital_usd = EXCLUDED.assigned_capital_usd,
                            target_leverage = EXCLUDED.target_leverage,
                            quote_asset = EXCLUDED.quote_asset,
                            observed_margin_mode = EXCLUDED.observed_margin_mode,
                            required_margin_mode = EXCLUDED.required_margin_mode,
                            balance_valid = EXCLUDED.balance_valid,
                            capital_band_valid = EXCLUDED.capital_band_valid,
                            leverage_valid = EXCLUDED.leverage_valid,
                            quote_asset_valid = EXCLUDED.quote_asset_valid,
                            margin_mode_valid = EXCLUDED.margin_mode_valid,
                            api_permissions_valid = EXCLUDED.api_permissions_valid,
                            manual_positions_valid = EXCLUDED.manual_positions_valid,
                            risk_policy_valid = EXCLUDED.risk_policy_valid,
                            reason_codes = EXCLUDED.reason_codes,
                            observed_at = EXCLUDED.observed_at,
                            validated_at = now(),
                            expires_at = EXCLUDED.expires_at,
                            updated_at = now()
                        """,
                UUID.randomUUID(), request.certificationId(), request.userId(), request.allocationId(),
                decision.valid() ? "VALID" : "REJECTED", request.balanceUsd(),
                request.assignedCapitalUsd(), request.targetLeverage(), request.quoteAsset(),
                request.observedMarginMode(), request.requiredMarginMode(), decision.balanceValid(),
                decision.capitalBandValid(), decision.leverageValid(), decision.quoteAssetValid(),
                decision.marginModeValid(), decision.apiPermissionsValid(),
                decision.manualPositionsValid(), decision.riskPolicyValid(),
                writeReasons(decision), request.observedAt(), request.expiresAt());
    }

    private String writeReasons(UserAdoptionValidationDecision decision) {
        try {
            return objectMapper.writeValueAsString(decision.reasonCodes());
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Adoption reasons are not serializable", ex);
        }
    }
}
