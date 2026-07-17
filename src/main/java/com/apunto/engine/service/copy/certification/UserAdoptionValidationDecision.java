package com.apunto.engine.service.copy.certification;

import java.util.List;

public record UserAdoptionValidationDecision(
        boolean valid,
        boolean balanceValid,
        boolean capitalBandValid,
        boolean leverageValid,
        boolean quoteAssetValid,
        boolean marginModeValid,
        boolean apiPermissionsValid,
        boolean manualPositionsValid,
        boolean riskPolicyValid,
        List<String> reasonCodes
) {
}
