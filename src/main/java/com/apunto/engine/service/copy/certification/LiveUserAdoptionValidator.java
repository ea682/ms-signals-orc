package com.apunto.engine.service.copy.certification;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Component
public class LiveUserAdoptionValidator {

    private final Clock clock;

    public LiveUserAdoptionValidator() {
        this(Clock.systemUTC());
    }

    LiveUserAdoptionValidator(Clock clock) {
        this.clock = clock;
    }

    public UserAdoptionValidationDecision validate(UserAdoptionValidationRequest request) {
        if (request == null || request.certificationIdentity() == null) {
            return invalidContract();
        }
        LiveCertificationIdentity identity = request.certificationIdentity();
        List<String> reasons = new ArrayList<>();

        boolean balanceValid = nonNegative(request.balanceUsd())
                && positive(request.assignedCapitalUsd())
                && request.balanceUsd().compareTo(request.assignedCapitalUsd()) >= 0;
        addIfFalse(reasons, balanceValid, "LIVE_ADOPTION_BALANCE_INSUFFICIENT");

        boolean capitalBandValid = positive(request.assignedCapitalUsd())
                && request.assignedCapitalUsd().compareTo(identity.capitalBandMin()) >= 0
                && request.assignedCapitalUsd().compareTo(identity.capitalBandMax()) <= 0;
        addIfFalse(reasons, capitalBandValid, "LIVE_ADOPTION_CAPITAL_BAND_MISMATCH");

        boolean leverageValid = positive(request.targetLeverage())
                && request.targetLeverage().compareTo(identity.targetLeverage()) == 0;
        addIfFalse(reasons, leverageValid, "LIVE_ADOPTION_LEVERAGE_MISMATCH");

        boolean quoteAssetValid = sameCode(request.quoteAsset(), identity.quoteAsset());
        addIfFalse(reasons, quoteAssetValid, "LIVE_ADOPTION_QUOTE_ASSET_MISMATCH");

        boolean marginModeValid = sameCode(request.observedMarginMode(), request.requiredMarginMode());
        addIfFalse(reasons, marginModeValid, "LIVE_ADOPTION_MARGIN_MODE_MISMATCH");
        addIfFalse(reasons, request.apiPermissionsValid(), "LIVE_ADOPTION_API_PERMISSIONS_INVALID");
        addIfFalse(reasons, request.manualPositionsValid(), "LIVE_ADOPTION_MANUAL_POSITIONS_CONFLICT");
        addIfFalse(reasons, request.riskPolicyValid(), "LIVE_ADOPTION_RISK_POLICY_REJECTED");

        OffsetDateTime now = OffsetDateTime.now(clock);
        boolean observedAtValid = request.observedAt() != null && !request.observedAt().isAfter(now);
        addIfFalse(reasons, observedAtValid, "LIVE_ADOPTION_OBSERVED_AT_IN_FUTURE");
        boolean expiryValid = request.expiresAt() != null
                && request.observedAt() != null
                && request.expiresAt().isAfter(request.observedAt())
                && request.expiresAt().isAfter(now);
        addIfFalse(reasons, expiryValid, "LIVE_ADOPTION_EXPIRED");

        return new UserAdoptionValidationDecision(
                reasons.isEmpty(), balanceValid, capitalBandValid, leverageValid, quoteAssetValid,
                marginModeValid, request.apiPermissionsValid(), request.manualPositionsValid(),
                request.riskPolicyValid(), List.copyOf(reasons));
    }

    private UserAdoptionValidationDecision invalidContract() {
        return new UserAdoptionValidationDecision(false, false, false, false, false,
                false, false, false, false, List.of("LIVE_ADOPTION_RUNTIME_CONTEXT_INCOMPLETE"));
    }

    private static void addIfFalse(List<String> reasons, boolean valid, String reason) {
        if (!valid) reasons.add(reason);
    }

    private static boolean nonNegative(BigDecimal value) {
        return value != null && value.signum() >= 0;
    }

    private static boolean positive(BigDecimal value) {
        return value != null && value.signum() > 0;
    }

    private static boolean sameCode(String left, String right) {
        return left != null && right != null && left.trim().equalsIgnoreCase(right.trim());
    }
}
