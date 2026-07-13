package com.apunto.engine.controller;

import com.apunto.engine.service.copy.certification.LiveCertificationCreateCommand;
import com.apunto.engine.service.copy.certification.LiveCertificationCreateResult;
import com.apunto.engine.service.copy.certification.LiveCertificationIdentity;
import com.apunto.engine.service.copy.certification.LiveCertificationStatus;
import com.apunto.engine.service.copy.certification.LiveCertificationTransitionCommand;
import com.apunto.engine.service.copy.certification.LiveCertificationTransitionResult;
import com.apunto.engine.service.copy.certification.LiveEvidenceLevel;
import com.apunto.engine.service.copy.certification.LiveUserAdoptionApplicationService;
import com.apunto.engine.service.copy.certification.LiveUserAdoptionCommand;
import com.apunto.engine.service.copy.certification.LiveUserAdoptionResult;
import com.apunto.engine.service.copy.certification.LiveAllocationActivationCommand;
import com.apunto.engine.service.copy.certification.LiveAllocationActivationResult;
import com.apunto.engine.service.copy.certification.ManualLiveAllocationActivationService;
import com.apunto.engine.service.copy.certification.ManualLiveCertificationCatalogService;
import com.apunto.engine.service.copy.certification.ManualLiveCertificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/v1/copy/certifications")
public class LiveCertificationController {

    private final ManualLiveCertificationCatalogService catalogService;
    private final ManualLiveCertificationService transitionService;
    private final LiveUserAdoptionApplicationService adoptionService;
    private final ManualLiveAllocationActivationService activationService;

    @PostMapping
    public LiveCertificationCreateResult create(@RequestBody CreateRequest request) {
        return catalogService.create(new LiveCertificationCreateCommand(
                request.identity(), request.evidenceLevel(), request.initialStatus(), request.actor(),
                request.reason(), request.evidenceSnapshot(), request.creationKey()));
    }

    @PostMapping("/{certificationId}/transitions")
    public LiveCertificationTransitionResult transition(
            @PathVariable UUID certificationId,
            @RequestBody TransitionRequest request) {
        return transitionService.transition(new LiveCertificationTransitionCommand(
                certificationId, request.expectedVersion(), request.expectedPriorStatus(),
                request.nextStatus(), false, request.actor(), request.reason(),
                request.evidenceSnapshot(), request.transitionKey()));
    }

    @PostMapping("/{certificationId}/adoptions")
    public LiveUserAdoptionResult validateAdoption(
            @PathVariable UUID certificationId,
            @RequestBody AdoptionRequest request) {
        return adoptionService.validateAndPersist(new LiveUserAdoptionCommand(
                certificationId, request.userId(), request.allocationId(), request.balanceUsd(),
                request.assignedCapitalUsd(), request.targetLeverage(), request.quoteAsset(),
                request.observedMarginMode(), request.requiredMarginMode(),
                request.apiPermissionsValid(), request.manualPositionsValid(),
                request.riskPolicyValid(), request.observedAt(), request.expiresAt()));
    }

    @PostMapping("/{certificationId}/allocations/{allocationId}/activate")
    public LiveAllocationActivationResult activateAllocation(
            @PathVariable UUID certificationId,
            @PathVariable Long allocationId,
            @RequestBody ActivationRequest request) {
        return activationService.activate(new LiveAllocationActivationCommand(
                allocationId, certificationId, request.actor(), request.reason(),
                request.activationKey()));
    }

    public record CreateRequest(
            LiveCertificationIdentity identity,
            LiveEvidenceLevel evidenceLevel,
            LiveCertificationStatus initialStatus,
            String actor,
            String reason,
            Map<String, Object> evidenceSnapshot,
            String creationKey
    ) {
    }

    public record TransitionRequest(
            long expectedVersion,
            LiveCertificationStatus expectedPriorStatus,
            LiveCertificationStatus nextStatus,
            String actor,
            String reason,
            Map<String, Object> evidenceSnapshot,
            String transitionKey
    ) {
    }

    public record AdoptionRequest(
            UUID userId,
            Long allocationId,
            BigDecimal balanceUsd,
            BigDecimal assignedCapitalUsd,
            BigDecimal targetLeverage,
            String quoteAsset,
            String observedMarginMode,
            String requiredMarginMode,
            boolean apiPermissionsValid,
            boolean manualPositionsValid,
            boolean riskPolicyValid,
            OffsetDateTime observedAt,
            OffsetDateTime expiresAt
    ) {
    }

    public record ActivationRequest(String actor, String reason, String activationKey) {
    }
}
