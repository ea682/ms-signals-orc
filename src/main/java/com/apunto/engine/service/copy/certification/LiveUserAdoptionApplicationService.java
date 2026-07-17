package com.apunto.engine.service.copy.certification;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LiveUserAdoptionApplicationService {

    private final LiveCertificationCatalogStore catalogStore;
    private final LiveUserAdoptionPersistenceService persistenceService;

    public LiveUserAdoptionResult validateAndPersist(LiveUserAdoptionCommand command) {
        if (command == null || command.certificationId() == null || command.userId() == null
                || command.allocationId() == null || command.allocationId() <= 0) {
            return LiveUserAdoptionResult.blocked("LIVE_ADOPTION_CONTRACT_INVALID");
        }
        LiveCertificationIdentity identity = catalogStore.findIdentityById(command.certificationId())
                .orElse(null);
        if (identity == null) {
            return LiveUserAdoptionResult.blocked("LIVE_CERTIFICATION_MISSING");
        }
        UserAdoptionValidationRequest request = new UserAdoptionValidationRequest(
                command.certificationId(), command.userId(), command.allocationId(), identity,
                command.balanceUsd(), command.assignedCapitalUsd(), command.targetLeverage(),
                command.quoteAsset(), command.observedMarginMode(), command.requiredMarginMode(),
                command.apiPermissionsValid(), command.manualPositionsValid(), command.riskPolicyValid(),
                command.observedAt(), command.expiresAt());
        return LiveUserAdoptionResult.persisted(persistenceService.validateAndPersist(request));
    }
}
