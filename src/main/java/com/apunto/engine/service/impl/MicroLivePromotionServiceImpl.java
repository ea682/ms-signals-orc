package com.apunto.engine.service.impl;

import com.apunto.engine.shared.metric.MetricStrategyIdentity;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.entity.CopyPromotionAuditEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.entity.UserWalletCopyPlanEntity;
import com.apunto.engine.repository.CopyDispatchIntentRepository;
import com.apunto.engine.repository.MicroLiveExecutionEvidenceProjection;
import com.apunto.engine.repository.CopyPromotionAuditRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.repository.UserWalletCopyPlanRepository;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;
import com.apunto.engine.service.MicroLivePromotionService;
import com.apunto.engine.service.copy.decision.CopyDecisionGateway;
import com.apunto.engine.service.copy.decision.CopyDecisionRequest;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageRequest;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolution;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolver;
import com.apunto.engine.service.copy.distribution.CopyDistributionUnitExecutor;
import com.apunto.engine.service.copy.distribution.CopyDistributionUnitExecutor.UnitMutationResult;
import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import com.apunto.engine.service.copy.promotion.LivePromotionResult;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver.CopyModeResolution;
import com.apunto.engine.service.copy.readiness.MicroLiveExecutionEvidence;
import com.apunto.engine.service.copy.readiness.MicroLiveExecutionEvidencePolicy;
import com.apunto.engine.service.copy.readiness.MicroLiveReadinessDecision;
import com.apunto.engine.service.copy.certification.AutomaticLiveCertificationResult;
import com.apunto.engine.service.copy.certification.AutomaticLiveCertificationService;
import com.apunto.engine.service.copy.certification.LiveUserRuntimeEligibilityGate;
import com.apunto.engine.service.copy.lifecycle.MicroLiveFlatness;
import com.apunto.engine.service.copy.lifecycle.MicroLiveFlatnessGate;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionOperations;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MicroLivePromotionServiceImpl implements MicroLivePromotionService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal HUNDRED = new BigDecimal("100");
    private static final BigDecimal LEGACY_MICRO_LIVE_ALLOCATION_PCT_SENTINEL = new BigDecimal("0.000001");

    private final UserCopyAllocationRepository allocationRepository;
    private final CopyDispatchIntentRepository dispatchIntentRepository;
    private final CopyPromotionAuditRepository auditRepository;
    private final LivePromotionProperties properties;
    private final CopyDecisionGateway copyDecisionGateway;
    private final MicroLiveExecutionEvidencePolicy executionEvidencePolicy;
    private final TransactionOperations transactionOperations;

    @Autowired(required = false)
    private LiveAllocationPercentageResolver liveAllocationPercentageResolver;

    @Autowired(required = false)
    private CopyDistributionUnitExecutor copyDistributionUnitExecutor;

    @Autowired(required = false)
    private UserWalletCopyPlanRepository planRepository;

    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    @Autowired(required = false)
    private AutomaticLiveCertificationService automaticLiveCertificationService;

    @Autowired(required = false)
    private DetailUserRepository detailUserRepository;

    @Autowired(required = false)
    private UserApiKeyRepository userApiKeyRepository;

    @Autowired(required = false)
    private MicroLiveFlatnessGate microLiveFlatnessGate;

    @Autowired(required = false)
    private LiveUserRuntimeEligibilityGate liveUserRuntimeEligibilityGate;

    @Autowired
    public MicroLivePromotionServiceImpl(UserCopyAllocationRepository allocationRepository,
                                         CopyDispatchIntentRepository dispatchIntentRepository,
                                         CopyPromotionAuditRepository auditRepository,
                                         LivePromotionProperties properties,
                                         CopyDecisionGateway copyDecisionGateway,
                                         MicroLiveExecutionEvidencePolicy executionEvidencePolicy,
                                         PlatformTransactionManager transactionManager) {
        this(allocationRepository, dispatchIntentRepository, auditRepository, properties, copyDecisionGateway,
                executionEvidencePolicy, new TransactionTemplate(transactionManager));
    }

    MicroLivePromotionServiceImpl(UserCopyAllocationRepository allocationRepository,
                                  CopyDispatchIntentRepository dispatchIntentRepository,
                                  CopyPromotionAuditRepository auditRepository,
                                  LivePromotionProperties properties,
                                  CopyDecisionGateway copyDecisionGateway,
                                  MicroLiveExecutionEvidencePolicy executionEvidencePolicy,
                                  TransactionOperations transactionOperations) {
        this.allocationRepository = allocationRepository;
        this.dispatchIntentRepository = dispatchIntentRepository;
        this.auditRepository = auditRepository;
        this.properties = properties;
        this.copyDecisionGateway = copyDecisionGateway;
        this.executionEvidencePolicy = executionEvidencePolicy;
        this.transactionOperations = transactionOperations;
    }

    @Override
    public LivePromotionResult promoteMicroLiveToLive() {
        if (properties == null || !properties.isEnabled()) {
            log.info("event=copy.promotion.micro_to_live.skipped reason=PROMOTION_DISABLED");
            return LivePromotionResult.empty();
        }
        if (properties.isManualCertificationRequired()) {
            log.warn("event=copy.promotion.micro_to_live.configuration_deprecated reasonCode=AUTOMATIC_REAL_EVIDENCE_CERTIFICATION_REQUIRED configuredManualCertificationRequired=true");
        }

        List<UserCopyAllocationEntity> candidates = allocationRepository.findMicroLivePromotionCandidates(
                Math.max(1, properties.getCandidateLimit())
        );
        if (candidates == null || candidates.isEmpty()) {
            log.info("event=copy.promotion.micro_to_live.started candidates=0");
            return LivePromotionResult.empty();
        }

        int evaluated = 0;
        int ready = 0;
        int promoted = 0;
        int rejected = 0;
        int skipped = 0;
        OffsetDateTime now = OffsetDateTime.now();
        Map<Long, MicroLiveExecutionEvidence> executionEvidence = loadExecutionEvidence(candidates, now);
        log.info("event=copy.promotion.micro_to_live.started candidates={}", candidates.size());

        for (UserCopyAllocationEntity allocation : candidates) {
            if (allocation == null || allocation.getId() == null) {
                skipped++;
                continue;
            }
            evaluated++;
            try {
                LiveDecision decision = evaluate(allocation, now, executionEvidence.get(allocation.getId()));
                if (!decision.allowed() && "LIVE_ALLOCATION_ALREADY_EXISTS".equals(decision.reasonCode())) {
                    skipped++;
                    inTransaction(() -> {
                        audit(allocation, decision, "MICRO_LIVE_EVALUATED");
                        audit(allocation, decision, "MICRO_LIVE_PROMOTION_NOOP");
                    });
                    log.info(
                            "event=copy.promotion.micro_to_live.noop userId={} walletId={} microLiveAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=LIVE decision=NOOP reasonCode={}",
                            allocation.getIdUser(),
                            allocation.getWalletId(),
                            allocation.getId(),
                            allocation.getCopyStrategyCode(),
                            allocation.getScopeType(),
                            allocation.getScopeValue(),
                            safe(allocation.getCopyMode()),
                            decision.resolvedCopyMode(),
                            decision.reasonCode()
                    );
                    continue;
                }
                if (!decision.allowed()) {
                    rejected++;
                    inTransaction(() -> {
                        allocation.setStatusReason(decision.reasonCode());
                        allocation.setStatusUpdatedAt(now);
                        allocation.setUpdatedAt(now);
                        allocationRepository.save(allocation);
                        audit(allocation, decision, "MICRO_LIVE_EVALUATED");
                        audit(allocation, decision, "MICRO_LIVE_PROMOTION_REJECTED");
                    });
                    if (isLivePercentageReason(decision.reasonCode())) {
                        recordLivePromotion("rejected", decision.reasonCode());
                        LiveAllocationPercentageResolution percentage = decision.percentageResolution();
                        log.info("event=copy.promotion.live.rejected executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} previousExecutionMode=MICRO_LIVE previousAllocationPct={} resolvedLiveAllocationPct={} allocationPctSource={} decision=KEEP_MICRO_LIVE reasonCode={} retryable=true shouldAlert=false recommendedAction=WAIT_FOR_NEXT_VALID_DISTRIBUTION",
                                allocation.getIdUser(), allocation.getWalletId(), allocation.getCopyStrategyCode(),
                                allocation.getScopeType(), allocation.getScopeValue(), allocation.getAllocationPct(),
                                percentage == null ? null : percentage.percentage(),
                                percentage == null ? null : percentage.source(), decision.reasonCode());
                    }
                    log.info(
                            "event=copy.promotion.micro_to_live.rejected userId={} walletId={} microLiveAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=MICRO_LIVE decision=REJECT reasonCode={} details={}",
                            allocation.getIdUser(),
                            allocation.getWalletId(),
                            allocation.getId(),
                            allocation.getCopyStrategyCode(),
                            allocation.getScopeType(),
                            allocation.getScopeValue(),
                            safe(allocation.getCopyMode()),
                            decision.resolvedCopyMode(),
                            decision.reasonCode(),
                            decision.details()
                    );
                    continue;
                }

                ready++;
                PromotionMutation promotion = promoteUnderProfileLock(allocation, decision, now);
                if (!promotion.created()) {
                    if (promotion.liveAllocation() != null) {
                        skipped++;
                    } else {
                        rejected++;
                    }
                    log.info("event=copy.promotion.micro_to_live.conditional_noop userId={} walletId={} microLiveAllocationId={} strategyCode={} scopeType={} scopeValue={} decision={} reasonCode={}",
                            allocation.getIdUser(), allocation.getWalletId(), allocation.getId(),
                            allocation.getCopyStrategyCode(), allocation.getScopeType(), allocation.getScopeValue(),
                            promotion.liveAllocation() == null ? "KEEP_MICRO_LIVE" : "NOOP",
                            promotion.reasonCode());
                    continue;
                }
                UserCopyAllocationEntity closedMicro = promotion.closedMicro();
                UserCopyAllocationEntity liveAllocation = promotion.liveAllocation();
                promoted++;
                log.info(
                        "event=copy.promotion.micro_to_live.created userId={} walletId={} microLiveAllocationId={} liveAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=LIVE decision=CREATED reasonCode={} events={} errorRatePct={} netPnlUsd={}",
                        closedMicro.getIdUser(),
                        liveAllocation.getWalletId(),
                        closedMicro.getId(),
                        liveAllocation.getId(),
                        liveAllocation.getCopyStrategyCode(),
                        liveAllocation.getScopeType(),
                        liveAllocation.getScopeValue(),
                        safe(closedMicro.getCopyMode()),
                        liveAllocation.getCopyMode(),
                        decision.reasonCode(),
                        decision.events(),
                        decision.errorRatePct(),
                        decision.netPnlUsd()
                );
                LiveAllocationPercentageResolution percentage = promotion.percentageResolution();
                log.info("event=copy.promotion.live.percentage.resolved executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} previousExecutionMode=MICRO_LIVE previousAllocationPct={} resolvedLiveAllocationPct={} walletTotalAllocationPct={} allocationPctSource={} distributionDecisionId={} distributionCalculatedAt={} usesAllocationPctForSizing=true decision=PROMOTE reasonCode=LIVE_ALLOCATION_PCT_RESOLVED",
                        closedMicro.getIdUser(), liveAllocation.getWalletId(), liveAllocation.getCopyStrategyCode(),
                        liveAllocation.getScopeType(), liveAllocation.getScopeValue(), closedMicro.getAllocationPct(),
                        percentage.percentage(), percentage.walletTotalPercentage(), percentage.source(),
                        percentage.sourceId(), percentage.calculatedAt());
            } catch (DataIntegrityViolationException ex) {
                Optional<UserCopyAllocationEntity> existing = existingLive(allocation);
                if (existing.isPresent()) {
                    skipped++;
                    LiveDecision noop = rejected("LIVE_ALLOCATION_ALREADY_EXISTS", allocation, 0, 0, ZERO, ZERO, 0, safe(existing.get().getCopyMode()));
                    inTransaction(() -> audit(allocation, noop, "MICRO_LIVE_PROMOTION_NOOP"));
                    log.info(
                            "event=copy.promotion.micro_to_live.noop userId={} walletId={} microLiveAllocationId={} liveAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=LIVE decision=NOOP reasonCode=LIVE_ALLOCATION_ALREADY_EXISTS errorClass={}",
                            allocation.getIdUser(),
                            allocation.getWalletId(),
                            allocation.getId(),
                            existing.get().getId(),
                            allocation.getCopyStrategyCode(),
                            allocation.getScopeType(),
                            allocation.getScopeValue(),
                            safe(allocation.getCopyMode()),
                            existing.get().getCopyMode(),
                            ex.getClass().getSimpleName()
                    );
                    continue;
                }
                rejected++;
                LiveDecision failed = rejected("PROMOTION_FAILED_DUPLICATE_CONSTRAINT", allocation, 0, 0, ZERO, ZERO, 0, null);
                inTransaction(() -> audit(allocation, failed, "MICRO_LIVE_PROMOTION_REJECTED"));
                log.warn(
                        "event=copy.promotion.micro_to_live.rejected userId={} walletId={} microLiveAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=MICRO_LIVE decision=REJECT reasonCode={} errorClass={} errorMessage=\"{}\"",
                        allocation.getIdUser(),
                        allocation.getWalletId(),
                        allocation.getId(),
                        allocation.getCopyStrategyCode(),
                        allocation.getScopeType(),
                        allocation.getScopeValue(),
                        safe(allocation.getCopyMode()),
                        null,
                        failed.reasonCode(),
                        ex.getClass().getSimpleName(),
                        safe(ex.getMessage())
                );
            } catch (RuntimeException ex) {
                rejected++;
                LiveDecision failed = rejected("PROMOTION_FAILED", allocation, 0, 0, ZERO, ZERO, 0, null);
                inTransaction(() -> audit(allocation, failed, "MICRO_LIVE_PROMOTION_REJECTED"));
                log.warn(
                        "event=copy.promotion.micro_to_live.rejected userId={} walletId={} microLiveAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=MICRO_LIVE decision=REJECT reasonCode={} errorClass={} errorMessage=\"{}\"",
                        allocation.getIdUser(),
                        allocation.getWalletId(),
                        allocation.getId(),
                        allocation.getCopyStrategyCode(),
                        allocation.getScopeType(),
                        allocation.getScopeValue(),
                        safe(allocation.getCopyMode()),
                        null,
                        failed.reasonCode(),
                        ex.getClass().getSimpleName(),
                        safe(ex.getMessage())
                );
            }
        }

        log.info(
                "event=copy.promotion.micro_to_live.finished evaluated={} ready={} promoted={} rejected={} skipped={}",
                evaluated, ready, promoted, rejected, skipped
        );
        return new LivePromotionResult(evaluated, ready, promoted, rejected, skipped);
    }

    private PromotionMutation promoteUnderProfileLock(
            UserCopyAllocationEntity candidate,
            LiveDecision decision,
            OffsetDateTime now
    ) {
        if (copyDistributionUnitExecutor == null) {
            return inTransaction(() -> mutatePromotion(candidate, decision, now));
        }
        PromotionMutation[] outcome = new PromotionMutation[1];
        copyDistributionUnitExecutor.execute(
                candidate.getIdUser(),
                candidate.getWalletId(),
                profileKey(candidate),
                candidate.getId(),
                () -> {
                    UserCopyAllocationEntity current = allocationRepository.findById(candidate.getId())
                            .orElseThrow(() -> new IllegalStateException("MICRO_LIVE_ALLOCATION_MISSING"));
                    outcome[0] = mutatePromotion(current, decision, now);
                    return outcome[0].created()
                            ? new UnitMutationResult(1, 1, 1, 1, 0L)
                            : UnitMutationResult.persisted(false, false);
                });
        return java.util.Objects.requireNonNull(outcome[0], "promotion outcome");
    }

    private PromotionMutation mutatePromotion(
            UserCopyAllocationEntity micro,
            LiveDecision decision,
            OffsetDateTime now
    ) {
        Optional<UserCopyAllocationEntity> existing = existingLive(micro);
        if (existing.isPresent() && !isRecertificationCandidate(existing.get())) {
            LiveDecision noop = decisionWithResolution(
                    decision,
                    false,
                    recertificationBlockedReason(existing.get()),
                    decision.percentageResolution());
            audit(micro, noop, "MICRO_LIVE_PROMOTION_NOOP");
            return PromotionMutation.noop(micro, existing.get());
        }
        if (!"MICRO_LIVE".equals(UserCopyAllocationEntity.normalizeExecutionMode(micro.getExecutionMode()))
                || !micro.isActive()
                || micro.getEndsAt() != null
                || micro.getStatus() != UserCopyAllocationEntity.Status.ACTIVE) {
            return PromotionMutation.rejected(micro, "MICRO_LIVE_STATE_CHANGED");
        }

        OffsetDateTime recheckTime = OffsetDateTime.now();
        LiveAllocationPercentageResolution currentPercentage = resolveLivePercentage(micro, recheckTime);
        String reasonCode = livePercentageFailureReason(currentPercentage, recheckTime);
        if (reasonCode != null) {
            LiveDecision blocked = decisionWithResolution(decision, false, reasonCode, currentPercentage);
            micro.setStatusReason(reasonCode);
            micro.setStatusUpdatedAt(recheckTime);
            micro.setUpdatedAt(recheckTime);
            allocationRepository.save(micro);
            audit(micro, blocked, "MICRO_LIVE_EVALUATED");
            audit(micro, blocked, "MICRO_LIVE_PROMOTION_REJECTED");
            recordLivePromotion("rejected", reasonCode);
            log.info("event=copy.promotion.live.rejected executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} previousExecutionMode=MICRO_LIVE previousAllocationPct={} resolvedLiveAllocationPct={} allocationPctSource={} decision=KEEP_MICRO_LIVE reasonCode={} retryable=true shouldAlert=false recommendedAction=WAIT_FOR_NEXT_VALID_DISTRIBUTION",
                    micro.getIdUser(), micro.getWalletId(), micro.getCopyStrategyCode(), micro.getScopeType(),
                    micro.getScopeValue(), micro.getAllocationPct(),
                    currentPercentage == null ? null : currentPercentage.percentage(),
                    currentPercentage == null ? null : currentPercentage.source(), reasonCode);
            return PromotionMutation.rejected(micro, reasonCode);
        }

        LiveDecision effectiveDecision = decisionWithResolution(
                decision, true, "MICRO_LIVE_VALIDATED_READY_FOR_LIVE", currentPercentage);
        UUID certificationId = null;
        if (automaticLiveCertificationService != null) {
            AutomaticLiveCertificationResult certification = automaticLiveCertificationService.certify(
                    micro, effectiveDecision.details());
            if (!certification.approved()) {
                micro.setStatusReason(certification.reasonCode());
                micro.setStatusUpdatedAt(recheckTime);
                micro.setUpdatedAt(recheckTime);
                allocationRepository.save(micro);
                audit(micro, decisionWithResolution(
                        decision, false, certification.reasonCode(), currentPercentage),
                        "MICRO_LIVE_PROMOTION_REJECTED");
                return PromotionMutation.rejected(micro, certification.reasonCode());
            }
            certificationId = certification.certificationId();
        }
        if (detailUserRepository != null) {
            DetailUserEntity detail = detailUserRepository.findByUser_Id(micro.getIdUser());
            if (detail == null || !detail.isContinueInLiveAfterCertification()) {
                LiveDecision declined = decisionWithResolution(
                        decision, false, "LIVE_CONTINUATION_NOT_REQUESTED", currentPercentage);
                UserCopyAllocationEntity closedWithoutLive = completeOrDeriskMicro(
                        micro,
                        "MICRO_LIVE_CERTIFIED_USER_DECLINED_LIVE",
                        "MICRO_LIVE_CERTIFIED_EXIT_ONLY_PENDING_FLAT",
                        now);
                audit(closedWithoutLive, declined, "MICRO_LIVE_CERTIFIED_NO_LIVE");
                return PromotionMutation.rejected(closedWithoutLive, "LIVE_CONTINUATION_NOT_REQUESTED");
            }
        }
        UserApiKeyEntity liveAccount = userApiKeyRepository == null ? null : userApiKeyRepository
                .findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(
                        micro.getIdUser(), "BINANCE", ExecutionAccountPurpose.LIVE)
                .orElse(null);
        if (userApiKeyRepository != null && !usable(liveAccount)) {
            micro.setStatusReason("LIVE_EXECUTION_ACCOUNT_MISSING");
            micro.setStatusUpdatedAt(now);
            micro.setUpdatedAt(now);
            allocationRepository.save(micro);
            audit(micro, decisionWithResolution(decision, false,
                    "LIVE_EXECUTION_ACCOUNT_MISSING", currentPercentage),
                    "MICRO_LIVE_PROMOTION_REJECTED");
            return PromotionMutation.rejected(micro, "LIVE_EXECUTION_ACCOUNT_MISSING");
        }
        if (existing.isPresent()) {
            UserCopyAllocationEntity live = existing.get();
            if (certificationId != null) live.setLiveCertificationId(certificationId);
            LiveUserRuntimeEligibilityGate.Decision userEligibility = liveUserRuntimeEligibilityGate == null
                    ? LiveUserRuntimeEligibilityGate.Decision.block("LIVE_USER_ELIGIBILITY_UNAVAILABLE")
                    : liveUserRuntimeEligibilityGate.evaluateForActivation(live);
            if (userEligibility == null || !userEligibility.allowed()) {
                String eligibilityReason = userEligibility == null
                        ? "LIVE_USER_ELIGIBILITY_UNAVAILABLE"
                        : userEligibility.reasonCode();
                micro.setStatusReason(eligibilityReason);
                micro.setStatusUpdatedAt(now);
                micro.setUpdatedAt(now);
                allocationRepository.save(micro);
                audit(micro, decisionWithResolution(
                        decision, false, eligibilityReason, currentPercentage),
                        "MICRO_LIVE_RECERTIFICATION_REJECTED");
                return PromotionMutation.rejected(micro, eligibilityReason);
            }
            if (liveAccount != null && live.getExchangeAccountId() != null
                    && !live.getExchangeAccountId().equals(liveAccount.getId())) {
                return PromotionMutation.rejected(micro, "EXECUTION_ACCOUNT_PURPOSE_MISMATCH");
            }
            if (live.getExchangeAccountId() == null && liveAccount != null) {
                live.setExchangeAccountId(liveAccount.getId());
            }
            applyLivePercentage(live, currentPercentage);
            live.setCopyMode(effectiveDecision.resolvedCopyMode());
            live.setStatus(UserCopyAllocationEntity.Status.ACTIVE);
            live.setActive(true);
            live.setEndsAt(null);
            live.setStatusReason("LIVE_RECERTIFIED");
            live.setStatusUpdatedAt(now);
            live.setUpdatedAt(now);
            live.setActivationAt(now);
            UserCopyAllocationEntity closed = completeOrDeriskMicro(
                    micro,
                    "MICRO_LIVE_RECERTIFICATION_COMPLETED",
                    "MICRO_LIVE_RECERTIFICATION_EXIT_ONLY_PENDING_FLAT",
                    now);
            UserCopyAllocationEntity reactivated = allocationRepository.saveAndFlush(live);
            audit(closed, effectiveDecision, "MICRO_LIVE_RECERTIFICATION_COMPLETED");
            auditLiveRecertified(closed, reactivated, effectiveDecision);
            recordLivePromotion("recertified", "LIVE_RECERTIFIED");
            return PromotionMutation.created(closed, reactivated, currentPercentage);
        }
        UserCopyAllocationEntity closed = completeOrDeriskMicro(
                micro,
                "PROMOTED_MICRO_TO_LIVE_CLOSED",
                "MICRO_LIVE_PROMOTED_EXIT_ONLY_PENDING_FLAT",
                now);
        UserCopyAllocationEntity live = allocationRepository.saveAndFlush(liveFromMicro(
                closed, now, effectiveDecision.resolvedCopyMode(), currentPercentage, certificationId,
                automaticLiveCertificationService != null, liveAccount == null ? null : liveAccount.getId()));
        updateLivePlan(closed, currentPercentage, effectiveDecision, now);
        audit(closed, effectiveDecision, "MICRO_LIVE_EVALUATED");
        auditMicroClosed(closed, effectiveDecision);
        auditLiveCreated(closed, live, effectiveDecision);
        recordLivePromotion("promoted", "LIVE_ALLOCATION_PCT_RESOLVED");
        return PromotionMutation.created(closed, live, currentPercentage);
    }

    private UserCopyAllocationEntity completeOrDeriskMicro(UserCopyAllocationEntity micro,
                                                           String closedReason,
                                                           String exitOnlyReason,
                                                           OffsetDateTime now) {
        MicroLiveFlatness flatness;
        try {
            flatness = microLiveFlatnessGate == null
                    ? new MicroLiveFlatness(true, 0, 0)
                    : microLiveFlatnessGate.evaluate(micro.getId());
        } catch (RuntimeException error) {
            flatness = new MicroLiveFlatness(false, -1, -1);
            log.warn("event=copy.promotion.micro_flatness_failed allocationId={} reasonCode=MICRO_LIVE_FLATNESS_UNAVAILABLE decision=KEEP_EXIT_ONLY errorClass={}",
                    micro.getId(), error.getClass().getSimpleName());
        }
        if (flatness.flat()) {
            micro.setStatus(UserCopyAllocationEntity.Status.CLOSED);
            micro.setActive(false);
            micro.setEndsAt(now);
            micro.setStatusReason(closedReason);
        } else {
            micro.setStatus(UserCopyAllocationEntity.Status.EXIT_ONLY);
            micro.setActive(true);
            micro.setEndsAt(null);
            micro.setStatusReason(exitOnlyReason);
        }
        micro.setStatusUpdatedAt(now);
        micro.setUpdatedAt(now);
        log.info("event=copy.promotion.micro_completion_state allocationId={} status={} active={} activePositions={} pendingDispatches={} reserveReleased={} reasonCode={}",
                micro.getId(), micro.getStatus(), micro.isActive(), flatness.activePositions(),
                flatness.pendingDispatches(), flatness.flat(), micro.getStatusReason());
        return allocationRepository.saveAndFlush(micro);
    }

    private void updateLivePlan(
            UserCopyAllocationEntity micro,
            LiveAllocationPercentageResolution resolution,
            LiveDecision decision,
            OffsetDateTime now
    ) {
        if (planRepository == null) return;
        String walletLc = normalizeWallet(micro.getWalletId());
        planRepository.ensureFixedBudgetPlan(micro.getIdUser(), walletLc, now);
        UserWalletCopyPlanEntity plan = planRepository.findForUpdate(micro.getIdUser(), walletLc)
                .orElseThrow(() -> new IllegalStateException("USER_WALLET_COPY_PLAN_MISSING_AFTER_UPSERT"));
        plan.setAllocationPct(resolution.walletTotalPercentage());
        plan.setWalletTotalAllocationPct(resolution.walletTotalPercentage());
        plan.setSizingMode("PERCENTAGE");
        plan.setAllocationPctSource(resolution.source());
        plan.setAllocationPctSourceId(resolution.sourceId());
        plan.setAllocationPctCalculatedAt(resolution.calculatedAt());
        plan.setAllocationPctValidUntil(resolution.validUntil());
        plan.setStatus("ACTIVE");
        plan.setActive(true);
        plan.setSyncedToRuntime(true);
        plan.setRuntimeSyncedAt(now);
        plan.setUpdatedAt(now);
        if (plan.getUserCapitalUsd() != null && plan.getUserCapitalUsd().signum() >= 0) {
            plan.setAllocatedCapitalUsd(plan.getUserCapitalUsd()
                    .multiply(resolution.walletTotalPercentage())
                    .setScale(8, RoundingMode.HALF_UP));
        } else {
            plan.setAllocatedCapitalUsd(null);
        }
        plan.setReason(new LinkedHashMap<>(decision.details()));
        planRepository.saveAndFlush(plan);
    }

    private LiveDecision decisionWithResolution(
            LiveDecision base,
            boolean allowed,
            String reasonCode,
            LiveAllocationPercentageResolution resolution
    ) {
        Map<String, Object> updatedDetails = new LinkedHashMap<>(base.details());
        appendPercentageResolution(updatedDetails, resolution);
        return new LiveDecision(allowed, reasonCode, updatedDetails, base.events(), base.errors(),
                base.errorRatePct(), base.netPnlUsd(), base.days(), base.resolvedCopyMode(), resolution);
    }

    private void recordLivePromotion(String result, String reason) {
        if (meterRegistry == null) return;
        meterRegistry.counter("copy_live_promotion_total",
                "result", safeMetricTag(result), "reason", safeMetricTag(reason)).increment();
    }

    private void detectLegacySentinel(UserCopyAllocationEntity allocation) {
        if (allocation.getAllocationPct() == null
                || allocation.getAllocationPct().compareTo(LEGACY_MICRO_LIVE_ALLOCATION_PCT_SENTINEL) != 0) {
            return;
        }
        if (meterRegistry != null) {
            meterRegistry.counter("copy_legacy_allocation_sentinel_total",
                    "execution_mode", "micro_live", "action", "ignored_for_sizing").increment();
        }
        log.warn("event=copy.allocation.legacy_sentinel.detected executionMode=MICRO_LIVE allocationId={} planId=null allocationPct=0.000001 decision=IGNORE_FOR_SIZING reasonCode=LEGACY_MICRO_LIVE_ALLOCATION_PCT_SENTINEL expected=allocationPct_null shouldAlert=false",
                allocation.getId());
    }

    private static String safeMetricTag(String value) {
        if (value == null || value.isBlank()) return "unknown";
        String normalized = value.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
        return normalized.length() > 80 ? normalized.substring(0, 80) : normalized;
    }

    private static boolean isLivePercentageReason(String reasonCode) {
        String normalized = normalizeToken(reasonCode);
        return normalized.startsWith("LIVE_ALLOCATION_PCT_")
                || normalized.startsWith("LIVE_DISTRIBUTION_");
    }

    private static String profileKey(UserCopyAllocationEntity allocation) {
        return MetricStrategyIdentity.canonicalKey(
                allocation.getWalletId(),
                allocation.getCopyStrategyCode(),
                allocation.getScopeType(),
                allocation.getScopeValue()
        );
    }

    private UserCopyAllocationEntity liveFromMicro(
            UserCopyAllocationEntity micro,
            OffsetDateTime now,
            String resolvedCopyMode,
            LiveAllocationPercentageResolution percentageResolution,
            UUID certificationId,
            boolean adoptionRequired,
            UUID exchangeAccountId
    ) {
        return UserCopyAllocationEntity.builder()
                .idUser(micro.getIdUser())
                .walletId(micro.getWalletId())
                .copyStrategyCode(micro.getCopyStrategyCode())
                .copyStrategySlug(micro.getCopyStrategySlug())
                .copyStrategyLabel(micro.getCopyStrategyLabel())
                .copyMode(resolvedCopyMode)
                .strategySourceEndpoint(micro.getStrategySourceEndpoint())
                .rankWithinStrategy(micro.getRankWithinStrategy())
                .globalRank(micro.getGlobalRank())
                .strategyScore(micro.getStrategyScore())
                .allocationPct(percentageResolution.percentage())
                .sizingMode("PERCENTAGE")
                .allocationPctSource(percentageResolution.source())
                .allocationPctSourceId(percentageResolution.sourceId())
                .allocationPctCalculatedAt(percentageResolution.calculatedAt())
                .allocationPctValidUntil(percentageResolution.validUntil())
                .walletTotalAllocationPct(percentageResolution.walletTotalPercentage())
                .score(micro.getScore())
                .status(adoptionRequired ? UserCopyAllocationEntity.Status.PAUSED : UserCopyAllocationEntity.Status.ACTIVE)
                .updatedAt(now)
                .isActive(true)
                .executionMode("LIVE")
                .exchangeAccountId(exchangeAccountId)
                .statusReason(adoptionRequired
                        ? "LIVE_ADOPTION_VALIDATION_REQUIRED"
                        : "PROMOTED_MICRO_TO_LIVE")
                .statusUpdatedAt(now)
                .leverageOverride(micro.getLeverageOverride())
                .copyMinNotionalMode(micro.getCopyMinNotionalMode())
                .copyMinNotionalMaxUsdt(micro.getCopyMinNotionalMaxUsdt())
                .copyMinNotionalMinScore(micro.getCopyMinNotionalMinScore())
                .copyMinNotionalMinHistoryDays(micro.getCopyMinNotionalMinHistoryDays())
                .copyMinNotionalMinOperations(micro.getCopyMinNotionalMinOperations())
                .scopeType(micro.getScopeType())
                .scopeValue(micro.getScopeValue())
                .strategyKey(micro.getStrategyKey())
                .walletProfileId(micro.getWalletProfileId())
                .linkedShadowAllocationId(micro.getLinkedShadowAllocationId())
                .promotedFromShadowAt(firstNonNull(micro.getPromotedFromShadowAt(), now))
                .activationAt(now)
                .liveCertificationId(certificationId)
                .sourceRankingVersion(micro.getSourceRankingVersion())
                .sourceSymbol(micro.getSourceSymbol())
                .targetSymbol(micro.getTargetSymbol())
                .capitalAsset(micro.getCapitalAsset())
                .resolvedQuoteAsset(micro.getResolvedQuoteAsset())
                .symbolResolutionStatus(micro.getSymbolResolutionStatus())
                .symbolResolutionReason(micro.getSymbolResolutionReason())
                .build();
    }

    private static boolean usable(UserApiKeyEntity account) {
        return account != null && account.isActive()
                && account.getApiKey() != null && !account.getApiKey().isBlank()
                && account.getApiSecret() != null && !account.getApiSecret().isBlank();
    }

    private Map<Long, MicroLiveExecutionEvidence> loadExecutionEvidence(List<UserCopyAllocationEntity> candidates,
                                                                        OffsetDateTime now) {
        Map<Long, UserCopyAllocationEntity> allocationsById = candidates.stream()
                .filter(java.util.Objects::nonNull)
                .filter(candidate -> candidate.getId() != null)
                .collect(Collectors.toMap(UserCopyAllocationEntity::getId, candidate -> candidate, (left, right) -> left));
        if (allocationsById.isEmpty()) return Map.of();

        Map<Long, MicroLiveExecutionEvidenceProjection> projections;
        try {
            List<MicroLiveExecutionEvidenceProjection> values = dispatchIntentRepository
                    .findMicroLiveExecutionEvidence(List.copyOf(allocationsById.keySet()));
            projections = values == null ? Map.of() : values.stream()
                    .filter(java.util.Objects::nonNull)
                    .filter(value -> value.getAllocationId() != null)
                    .collect(Collectors.toMap(MicroLiveExecutionEvidenceProjection::getAllocationId,
                            value -> value, (left, right) -> left));
        } catch (RuntimeException failure) {
            log.error("event=copy.promotion.micro_to_live.evidence_failed candidates={} reasonCode=MICRO_LIVE_EVIDENCE_QUERY_FAILED errorClass={} errorMessage=\"{}\" decision=FAIL_CLOSED",
                    allocationsById.size(), failure.getClass().getSimpleName(), safe(failure.getMessage()));
            projections = Map.of();
        }

        Map<Long, MicroLiveExecutionEvidence> result = new LinkedHashMap<>();
        for (Map.Entry<Long, UserCopyAllocationEntity> entry : allocationsById.entrySet()) {
            MicroLiveExecutionEvidenceProjection projection = projections.get(entry.getKey());
            result.put(entry.getKey(), projection == null
                    ? zeroEvidence(entry.getValue(), now)
                    : evidence(entry.getValue(), projection, now));
        }
        return Map.copyOf(result);
    }

    private MicroLiveExecutionEvidence evidence(UserCopyAllocationEntity allocation,
                                                MicroLiveExecutionEvidenceProjection projection,
                                                OffsetDateTime now) {
        OffsetDateTime since = firstNonNull(projection.getFirstSubmittedAt(),
                allocation.getPromotedFromShadowAt(), allocation.getUpdatedAt(), now);
        return MicroLiveExecutionEvidence.builder()
                .allocationId(allocation.getId())
                .observedDays(Math.max(0L, Duration.between(since, now).toDays()))
                .submittedOrders(longValue(projection.getSubmittedOrders()))
                .acknowledgedOrders(longValue(projection.getAcknowledgedOrders()))
                .filledOrders(longValue(projection.getFilledOrders()))
                .closedOperations(longValue(projection.getClosedOperations()))
                .dispatchErrors(longValue(projection.getDispatchErrors()))
                .reconciliationPending(longValue(projection.getReconciliationPending()))
                .duplicateCount(longValue(projection.getDuplicateCount()))
                .unresolvedAmbiguousTimeouts(longValue(projection.getUnresolvedAmbiguousTimeouts()))
                .slippageSamples(longValue(projection.getSlippageSamples()))
                .realizedPnlUsd(nullToZero(projection.getRealizedPnlUsd()))
                .maxDrawdownUsd(nullToZero(projection.getMaxDrawdownUsd()))
                .adverseSlippageP95Bps(nullToZero(projection.getAdverseSlippageP95Bps()))
                .firstSubmittedAt(projection.getFirstSubmittedAt())
                .build();
    }

    private MicroLiveExecutionEvidence zeroEvidence(UserCopyAllocationEntity allocation, OffsetDateTime now) {
        OffsetDateTime since = firstNonNull(allocation == null ? null : allocation.getPromotedFromShadowAt(),
                allocation == null ? null : allocation.getUpdatedAt(), now);
        return MicroLiveExecutionEvidence.builder()
                .allocationId(allocation == null ? null : allocation.getId())
                .observedDays(Math.max(0L, Duration.between(since, now).toDays()))
                .realizedPnlUsd(ZERO)
                .maxDrawdownUsd(ZERO)
                .adverseSlippageP95Bps(ZERO)
                .build();
    }

    private LiveDecision rejectedWithReadiness(String reasonCode,
                                               UserCopyAllocationEntity allocation,
                                               MicroLiveExecutionEvidence evidence,
                                               BigDecimal errorRate,
                                               MicroLiveReadinessDecision readiness) {
        LiveDecision base = rejected(reasonCode, allocation, evidence.submittedOrders(), evidence.dispatchErrors(),
                errorRate, nullToZero(evidence.realizedPnlUsd()), evidence.observedDays());
        Map<String, Object> enriched = new LinkedHashMap<>(base.details());
        appendExecutionEvidence(enriched, evidence, readiness);
        return new LiveDecision(base.allowed(), base.reasonCode(), enriched, base.events(), base.errors(),
                base.errorRatePct(), base.netPnlUsd(), base.days(), base.resolvedCopyMode());
    }

    private void appendExecutionEvidence(Map<String, Object> details,
                                         MicroLiveExecutionEvidence evidence,
                                         MicroLiveReadinessDecision readiness) {
        details.put("submittedOrders", evidence.submittedOrders());
        details.put("acknowledgedOrders", evidence.acknowledgedOrders());
        details.put("filledOrders", evidence.filledOrders());
        details.put("closedOperations", evidence.closedOperations());
        details.put("dispatchErrors", evidence.dispatchErrors());
        details.put("reconciliationPending", evidence.reconciliationPending());
        details.put("duplicateCount", evidence.duplicateCount());
        details.put("unresolvedAmbiguousTimeouts", evidence.unresolvedAmbiguousTimeouts());
        details.put("slippageSamples", evidence.slippageSamples());
        details.put("adverseSlippageP95Bps", evidence.adverseSlippageP95Bps());
        details.put("maxDrawdownUsd", evidence.maxDrawdownUsd());
        details.put("firstSubmittedAt", evidence.firstSubmittedAt());
        details.put("calendarProgressPct", readiness.calendarProgressPct());
        details.put("executionEvidencePct", readiness.executionEvidencePct());
        details.put("reconciliationPct", readiness.reconciliationPct());
        details.put("finalReadinessPct", readiness.finalReadinessPct());
        details.put("readinessReasons", readiness.reasons());
    }

    private static long longValue(Long value) {
        return value == null ? 0L : Math.max(0L, value);
    }

    private LiveDecision evaluate(UserCopyAllocationEntity allocation,
                                  OffsetDateTime now,
                                  MicroLiveExecutionEvidence rawEvidence) {
        if (!"MICRO_LIVE".equals(UserCopyAllocationEntity.normalizeExecutionMode(allocation.getExecutionMode()))) {
            return rejected("MICRO_LIVE_NOT_READY_WRONG_MODE", allocation, 0, 0, ZERO, ZERO, 0);
        }
        detectLegacySentinel(allocation);
        if (allocation.getIdUser() == null || allocation.getWalletId() == null) {
            return rejected("MICRO_LIVE_NOT_READY_INVALID_ALLOCATION", allocation, 0, 0, ZERO, ZERO, 0);
        }
        Optional<UserCopyAllocationEntity> existingLive = existingLive(allocation);
        if (existingLive.isPresent() && !isRecertificationCandidate(existingLive.get())) {
            return rejected(recertificationBlockedReason(existingLive.get()), allocation,
                    0, 0, ZERO, ZERO, 0, existingLive.get().getCopyMode());
        }

        CopyModeResolution copyModeResolution = UserCopyAllocationCopyModeResolver.resolve(
                allocation.getCopyStrategyCode(),
                allocation.getCopyMode()
        );
        log.info(
                "event=copy.promotion.micro_to_live.copy_mode.resolved userId={} walletId={} microLiveAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=LIVE decision={} reasonCode={} constraintReasonCode={}",
                allocation.getIdUser(),
                allocation.getWalletId(),
                allocation.getId(),
                allocation.getCopyStrategyCode(),
                allocation.getScopeType(),
                allocation.getScopeValue(),
                safe(allocation.getCopyMode()),
                copyModeResolution.copyMode(),
                copyModeResolution.valid() ? "ALLOW" : "REJECT",
                copyModeResolution.reasonCode(),
                copyModeResolution.constraintReasonCode()
        );
        if (!copyModeResolution.valid()) {
            return rejected(UserCopyAllocationCopyModeResolver.INVALID_COPY_MODE_MAPPING, allocation, 0, 0, ZERO, ZERO, 0, null);
        }

        MicroLiveExecutionEvidence evidence = rawEvidence == null
                ? zeroEvidence(allocation, now)
                : rawEvidence;
        long events = evidence.submittedOrders();
        long errors = evidence.dispatchErrors();
        BigDecimal pnl = nullToZero(evidence.realizedPnlUsd());
        BigDecimal errorRate = errorRatePct(events, errors);
        long days = evidence.observedDays();

        MicroLiveReadinessDecision readiness = executionEvidencePolicy.evaluate(evidence);
        if (!readiness.allowed()) {
            return rejectedWithReadiness(readiness.primaryReason(), allocation, evidence, errorRate, readiness);
        }

        LiveDecision fullDecision = evaluateFullDecisionForLive(allocation, events, errors, errorRate, pnl, days, copyModeResolution);
        if (!fullDecision.allowed()) {
            return fullDecision;
        }

        Map<String, Object> finalDetails = details(allocation, events, errors, errorRate, pnl, days, copyModeResolution.copyMode(), copyModeResolution.reasonCode(), copyModeResolution.constraintReasonCode());
        appendExecutionEvidence(finalDetails, evidence, readiness);
        finalDetails.putAll(fullDecision.details());
        LiveAllocationPercentageResolution percentageResolution = resolveLivePercentage(allocation, now);
        appendPercentageResolution(finalDetails, percentageResolution);
        String percentageFailure = livePercentageFailureReason(percentageResolution, now);
        if (percentageFailure != null) {
            return new LiveDecision(
                    false,
                    percentageFailure,
                    finalDetails,
                    events,
                    errors,
                    errorRate,
                    pnl,
                    days,
                    copyModeResolution.copyMode(),
                    percentageResolution
            );
        }
        return new LiveDecision(
                true,
                "MICRO_LIVE_VALIDATED_READY_FOR_LIVE",
                finalDetails,
                events,
                errors,
                errorRate,
                pnl,
                days,
                copyModeResolution.copyMode(),
                percentageResolution
        );
    }

    private LiveDecision evaluateFullDecisionForLive(
            UserCopyAllocationEntity allocation,
            long events,
            long errors,
            BigDecimal errorRate,
            BigDecimal pnl,
            long days,
            CopyModeResolution copyModeResolution
    ) {
        if (!properties.isRequireFullDecisionForLive()) {
            return new LiveDecision(
                    true,
                    "FULL_DECISION_REQUIREMENT_DISABLED",
                    Map.of("fullDecisionRequired", false),
                    events,
                    errors,
                    errorRate,
                    pnl,
                    days,
                    copyModeResolution.copyMode()
            );
        }
        CopyDecisionRequest request = new CopyDecisionRequest(
                normalizeWallet(allocation.getWalletId()),
                normalizeStrategy(allocation.getCopyStrategyCode()),
                normalizeScopeType(allocation.getScopeType()),
                normalizeScopeValue(allocation.getScopeValue(), allocation.getCopyStrategyCode()),
                "live-entry",
                "full",
                clampInt(properties.getCopyDecisionMinHistoryDays(), 1, 3650),
                clampInt(properties.getCopyDecisionSimulationLookbackDays(), 1, 3650),
                clampInt(properties.getCopyDecisionMaxFactsPerUnit(), 1, 50000),
                clampInt(properties.getCopyDecisionTimeoutMs(), 1, 59000),
                false
        );
        CopyDecisionDto decision;
        try {
            decision = copyDecisionGateway.getFullDecisionExact(request);
        } catch (RuntimeException ex) {
            log.warn(
                    "event=micro_live.promotion.full_decision.result userId={} walletId={} microLiveAllocationId={} decision=REJECT reasonCode=FULL_DECISION_FAILED errorClass={} errorMessage=\"{}\"",
                    allocation.getIdUser(),
                    allocation.getWalletId(),
                    allocation.getId(),
                    ex.getClass().getSimpleName(),
                    safe(ex.getMessage())
            );
            Map<String, Object> details = details(allocation, events, errors, errorRate, pnl, days, copyModeResolution.copyMode(), copyModeResolution.reasonCode(), copyModeResolution.constraintReasonCode());
            details.put("fullDecisionRequired", true);
            details.put("fullDecisionReasonCode", "FULL_DECISION_FAILED");
            details.put("fullDecisionErrorClass", ex.getClass().getSimpleName());
            details.put("fullDecisionError", safe(ex.getMessage()));
            return new LiveDecision(false, "FULL_DECISION_FAILED", details, events, errors, errorRate, pnl, days, copyModeResolution.copyMode());
        }

        FullDecisionGate gate = fullDecisionGate(decision, false);
        log.info(
                "event=micro_live.promotion.full_decision.result userId={} walletId={} microLiveAllocationId={} decision={} reasonCode={} fullMaterialized={} factPayloadLoaded={} decisionFinal={} requiresFullSimulation={} copyGuardAction={} canMicroLive={} canLive={}",
                allocation.getIdUser(),
                allocation.getWalletId(),
                allocation.getId(),
                gate.allowed() ? "ALLOW" : "REJECT",
                gate.reasonCode(),
                decision != null && decision.isFullMaterialized(),
                decision != null && decision.isFactPayloadLoaded(),
                decision != null && decision.isDecisionFinal(),
                decision != null && decision.isRequiresFullSimulation(),
                copyGuardAction(decision),
                decision != null && decision.isCanMicroLive(),
                decision != null && decision.isCanLive()
        );
        Map<String, Object> details = details(allocation, events, errors, errorRate, pnl, days, copyModeResolution.copyMode(), copyModeResolution.reasonCode(), copyModeResolution.constraintReasonCode());
        details.putAll(fullDecisionDetails(decision, gate));
        return new LiveDecision(gate.allowed(), gate.reasonCode(), details, events, errors, errorRate, pnl, days, copyModeResolution.copyMode());
    }

    private LiveAllocationPercentageResolution resolveLivePercentage(
            UserCopyAllocationEntity allocation,
            OffsetDateTime promotionTime
    ) {
        if (liveAllocationPercentageResolver == null) {
            return LiveAllocationPercentageResolution.rejected("LIVE_DISTRIBUTION_NOT_AVAILABLE");
        }
        try {
            return liveAllocationPercentageResolver.resolve(new LiveAllocationPercentageRequest(
                    allocation.getIdUser(),
                    allocation.getWalletId(),
                    allocation.getCopyStrategyCode(),
                    allocation.getScopeType(),
                    allocation.getScopeValue(),
                    promotionTime));
        } catch (RuntimeException ex) {
            log.warn("event=copy.promotion.live.rejected executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} previousExecutionMode=MICRO_LIVE previousAllocationPct={} resolvedLiveAllocationPct=null allocationPctSource=null decision=KEEP_MICRO_LIVE reasonCode=LIVE_DISTRIBUTION_NOT_AVAILABLE retryable=true shouldAlert=false recommendedAction=WAIT_FOR_NEXT_VALID_DISTRIBUTION errorClass={} errorMessage=\"{}\"",
                    allocation.getIdUser(), allocation.getWalletId(), allocation.getCopyStrategyCode(),
                    allocation.getScopeType(), allocation.getScopeValue(), allocation.getAllocationPct(),
                    ex.getClass().getSimpleName(), safe(ex.getMessage()));
            return LiveAllocationPercentageResolution.rejected("LIVE_DISTRIBUTION_NOT_AVAILABLE");
        }
    }

    private static String livePercentageFailureReason(
            LiveAllocationPercentageResolution resolution,
            OffsetDateTime promotionTime
    ) {
        if (resolution == null) return "LIVE_DISTRIBUTION_NOT_AVAILABLE";
        if (!resolution.resolved()) return resolution.reasonCode();
        if (resolution.percentage() == null) return "LIVE_ALLOCATION_PCT_MISSING";
        if (!resolution.validForLive()) return "LIVE_ALLOCATION_PCT_INVALID";
        OffsetDateTime effectiveTime = promotionTime == null ? OffsetDateTime.now() : promotionTime;
        if (resolution.calculatedAt().isAfter(effectiveTime)
                || !resolution.validUntil().isAfter(effectiveTime)) {
            return "LIVE_ALLOCATION_PCT_STALE";
        }
        return null;
    }

    private static void appendPercentageResolution(
            Map<String, Object> details,
            LiveAllocationPercentageResolution resolution
    ) {
        details.put("resolvedLiveAllocationPct", resolution == null ? null : resolution.percentage());
        details.put("walletTotalAllocationPct", resolution == null ? null : resolution.walletTotalPercentage());
        details.put("allocationPctSource", resolution == null ? null : resolution.source());
        details.put("distributionDecisionId", resolution == null ? null : resolution.sourceId());
        details.put("distributionCalculatedAt", resolution == null ? null : resolution.calculatedAt());
        details.put("distributionValidUntil", resolution == null ? null : resolution.validUntil());
        details.put("allocationPctReasonCode", resolution == null
                ? "LIVE_DISTRIBUTION_NOT_AVAILABLE"
                : resolution.reasonCode());
    }

    private LiveDecision rejected(
            String reason,
            UserCopyAllocationEntity allocation,
            long events,
            long errors,
            BigDecimal errorRate,
            BigDecimal pnl,
            long days
    ) {
        return rejected(reason, allocation, events, errors, errorRate, pnl, days, null);
    }

    private LiveDecision rejected(
            String reason,
            UserCopyAllocationEntity allocation,
            long events,
            long errors,
            BigDecimal errorRate,
            BigDecimal pnl,
            long days,
            String resolvedCopyMode
    ) {
        return new LiveDecision(false, reason, details(allocation, events, errors, errorRate, pnl, days, resolvedCopyMode, null, null), events, errors, errorRate, pnl, days, resolvedCopyMode);
    }

    private Optional<UserCopyAllocationEntity> existingLive(UserCopyAllocationEntity allocation) {
        return allocationRepository.findOpenLiveAllocationForUserWalletStrategyScope(
                allocation.getIdUser(),
                allocation.getWalletId(),
                allocation.getCopyStrategyCode(),
                allocation.getScopeType(),
                allocation.getScopeValue()
        );
    }

    private static boolean isRecertificationCandidate(UserCopyAllocationEntity live) {
        if (live == null || !live.isActive() || live.getEndsAt() != null
                || !"LIVE".equals(UserCopyAllocationEntity.normalizeExecutionMode(live.getExecutionMode()))) {
            return false;
        }
        String reason = live.getStatusReason() == null
                ? "" : live.getStatusReason().trim().toUpperCase(Locale.ROOT);
        if (reason.contains("REVOKED")) return false;
        return live.getStatus() != UserCopyAllocationEntity.Status.ACTIVE
                && (reason.startsWith("LIVE_CERTIFICATION_")
                || reason.startsWith("LIVE_RECERTIFICATION_"));
    }

    private static String recertificationBlockedReason(UserCopyAllocationEntity live) {
        String reason = live == null || live.getStatusReason() == null
                ? "" : live.getStatusReason().trim().toUpperCase(Locale.ROOT);
        return reason.contains("REVOKED")
                ? "LIVE_CERTIFICATION_REVOKED_TERMINAL"
                : "LIVE_ALLOCATION_ALREADY_EXISTS";
    }

    private static void applyLivePercentage(UserCopyAllocationEntity live,
                                            LiveAllocationPercentageResolution resolution) {
        live.setAllocationPct(resolution.percentage());
        live.setSizingMode("PERCENTAGE");
        live.setAllocationPctSource(resolution.source());
        live.setAllocationPctSourceId(resolution.sourceId());
        live.setAllocationPctCalculatedAt(resolution.calculatedAt());
        live.setAllocationPctValidUntil(resolution.validUntil());
        live.setWalletTotalAllocationPct(resolution.walletTotalPercentage());
    }

    private void audit(UserCopyAllocationEntity allocation, LiveDecision decision, String event) {
        auditRepository.save(CopyPromotionAuditEntity.builder()
                .idUser(allocation.getIdUser())
                .walletId(allocation.getWalletId())
                .copyStrategyCode(allocation.getCopyStrategyCode())
                .sourceExecutionMode("MICRO_LIVE")
                .targetExecutionMode(decision.allowed() ? "LIVE" : "MICRO_LIVE")
                .decision(event)
                .reasonCode(decision.reasonCode())
                .reasonDetails(decision.details())
                .shadowAllocationId(allocation.getLinkedShadowAllocationId())
                .microLiveAllocationId(allocation.getId())
                .liveAllocationId(decision.allowed() ? allocation.getId() : null)
                .build());
    }

    private void auditMicroClosed(UserCopyAllocationEntity microAllocation, LiveDecision decision) {
        auditRepository.save(CopyPromotionAuditEntity.builder()
                .idUser(microAllocation.getIdUser())
                .walletId(microAllocation.getWalletId())
                .copyStrategyCode(microAllocation.getCopyStrategyCode())
                .sourceExecutionMode("MICRO_LIVE")
                .targetExecutionMode("LIVE")
                .decision("MICRO_LIVE_CLOSED_FOR_LIVE")
                .reasonCode(decision.reasonCode())
                .reasonDetails(decision.details())
                .shadowAllocationId(microAllocation.getLinkedShadowAllocationId())
                .microLiveAllocationId(microAllocation.getId())
                .build());
    }

    private void auditLiveCreated(
            UserCopyAllocationEntity microAllocation,
            UserCopyAllocationEntity liveAllocation,
            LiveDecision decision
    ) {
        auditRepository.save(CopyPromotionAuditEntity.builder()
                .idUser(liveAllocation.getIdUser())
                .walletId(liveAllocation.getWalletId())
                .copyStrategyCode(liveAllocation.getCopyStrategyCode())
                .sourceExecutionMode("MICRO_LIVE")
                .targetExecutionMode("LIVE")
                .decision("LIVE_CREATED")
                .reasonCode(decision.reasonCode())
                .reasonDetails(decision.details())
                .shadowAllocationId(liveAllocation.getLinkedShadowAllocationId())
                .microLiveAllocationId(microAllocation.getId())
                .liveAllocationId(liveAllocation.getId())
                .build());
    }

    private void auditLiveRecertified(
            UserCopyAllocationEntity microAllocation,
            UserCopyAllocationEntity liveAllocation,
            LiveDecision decision
    ) {
        auditRepository.save(CopyPromotionAuditEntity.builder()
                .idUser(liveAllocation.getIdUser())
                .walletId(liveAllocation.getWalletId())
                .copyStrategyCode(liveAllocation.getCopyStrategyCode())
                .sourceExecutionMode("MICRO_LIVE")
                .targetExecutionMode("LIVE")
                .decision("LIVE_RECERTIFIED")
                .reasonCode("LIVE_RECERTIFIED")
                .reasonDetails(decision.details())
                .shadowAllocationId(liveAllocation.getLinkedShadowAllocationId())
                .microLiveAllocationId(microAllocation.getId())
                .liveAllocationId(liveAllocation.getId())
                .build());
    }

    private static Map<String, Object> details(
            UserCopyAllocationEntity allocation,
            long events,
            long errors,
            BigDecimal errorRate,
            BigDecimal pnl,
            long days
    ) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("allocationId", allocation.getId());
        details.put("walletId", allocation.getWalletId());
        details.put("strategyCode", allocation.getCopyStrategyCode());
        details.put("scopeType", allocation.getScopeType());
        details.put("scopeValue", allocation.getScopeValue());
        details.put("executionMode", allocation.getExecutionMode());
        details.put("sourceCopyMode", allocation.getCopyMode());
        details.put("microLiveDays", days);
        details.put("microLiveEvents", events);
        details.put("microLiveErrors", errors);
        details.put("microLiveErrorRatePct", errorRate);
        details.put("microLiveNetPnlUsd", pnl);
        return details;
    }

    private static Map<String, Object> details(
            UserCopyAllocationEntity allocation,
            long events,
            long errors,
            BigDecimal errorRate,
            BigDecimal pnl,
            long days,
            String resolvedCopyMode,
            String copyModeReasonCode,
            String copyModeConstraintReasonCode
    ) {
        Map<String, Object> details = details(allocation, events, errors, errorRate, pnl, days);
        if (resolvedCopyMode != null) details.put("resolvedCopyMode", resolvedCopyMode);
        if (copyModeReasonCode != null) details.put("copyModeReasonCode", copyModeReasonCode);
        if (copyModeConstraintReasonCode != null) details.put("copyModeConstraintReasonCode", copyModeConstraintReasonCode);
        return details;
    }

    private static FullDecisionGate fullDecisionGate(CopyDecisionDto decision, boolean requireMicroLive) {
        if (decision == null) {
            return new FullDecisionGate(false, "FULL_DECISION_FAILED");
        }
        if (!decision.isDecisionFinal()) {
            return new FullDecisionGate(false, firstNonBlank(decision.getReasonCode(), "FULL_DECISION_NOT_FINAL"));
        }
        if (decision.isRequiresFullSimulation() || !decision.isFullMaterialized()) {
            return new FullDecisionGate(false, firstNonBlank(decision.getReasonCode(), "FULL_DECISION_NOT_MATERIALIZED"));
        }
        if (!decision.isFactPayloadLoaded()) {
            return new FullDecisionGate(false, "FULL_FACT_PAYLOAD_NOT_LOADED");
        }
        if (!"ALLOW".equals(copyGuardAction(decision))) {
            return new FullDecisionGate(false, "FULL_DECISION_BLOCKED_BY_COPY_GUARD");
        }
        if (requireMicroLive && !decision.isCanMicroLive()) {
            return new FullDecisionGate(false, firstNonBlank(decision.getReasonCode(), "FULL_DECISION_CAN_MICRO_LIVE_FALSE"));
        }
        if (!requireMicroLive && !decision.isCanLive()) {
            return new FullDecisionGate(false, firstNonBlank(decision.getReasonCode(), "FULL_DECISION_CAN_LIVE_FALSE"));
        }
        return new FullDecisionGate(true, firstNonBlank(decision.getReasonCode(), requireMicroLive ? "FULL_DECISION_OK_FOR_MICRO_LIVE" : "FULL_DECISION_OK_FOR_LIVE"));
    }

    private static Map<String, Object> fullDecisionDetails(CopyDecisionDto decision, FullDecisionGate gate) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("fullDecisionRequired", true);
        details.put("fullDecisionReasonCode", gate.reasonCode());
        details.put("fullDecisionResponseReasonCode", decision == null ? null : decision.getReasonCode());
        details.put("fullDecisionResponseReasonDetail", decision == null ? null : decision.getReasonDetail());
        details.put("fullDecisionMode", decision == null ? null : decision.getMode());
        details.put("fullDecisionSimulationMode", decision == null ? null : decision.getSimulationMode());
        details.put("fullDecisionMaterialized", decision != null && decision.isFullMaterialized());
        details.put("fullDecisionFactPayloadLoaded", decision != null && decision.isFactPayloadLoaded());
        details.put("fullDecisionFinal", decision != null && decision.isDecisionFinal());
        details.put("fullDecisionRequiresFullSimulation", decision != null && decision.isRequiresFullSimulation());
        details.put("fullDecisionCanMicroLive", decision != null && decision.isCanMicroLive());
        details.put("fullDecisionCanLive", decision != null && decision.isCanLive());
        details.put("fullDecisionAllowNewEntries", decision != null && decision.isAllowNewEntries());
        details.put("fullDecisionCopyGuardStatus", copyGuardStatus(decision));
        details.put("fullDecisionCopyGuardAction", copyGuardAction(decision));
        details.put("fullDecisionCopyGuardReasons", copyGuardReasons(decision));
        details.put("fullDecisionElapsedMs", decision == null ? null : decision.getElapsedMs());
        details.put("fullDecisionFactsLoaded", decision == null ? null : decision.getFactsLoaded());
        return details;
    }

    private static String copyGuardAction(CopyDecisionDto decision) {
        return decision == null || decision.getCopyGuard() == null || decision.getCopyGuard().getAction() == null
                ? ""
                : normalizeToken(decision.getCopyGuard().getAction());
    }

    private static String copyGuardStatus(CopyDecisionDto decision) {
        return decision == null || decision.getCopyGuard() == null || decision.getCopyGuard().getStatus() == null
                ? ""
                : normalizeToken(decision.getCopyGuard().getStatus());
    }

    private static List<String> copyGuardReasons(CopyDecisionDto decision) {
        return decision == null || decision.getCopyGuard() == null || decision.getCopyGuard().getReasons() == null
                ? List.of()
                : decision.getCopyGuard().getReasons();
    }

    private static String normalizeWallet(String value) {
        return value == null ? null : value.trim().toLowerCase(Locale.ROOT);
    }

    private static String normalizeStrategy(String value) {
        if (value == null || value.isBlank()) return "MOVEMENT_ALL";
        return value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeScopeType(String value) {
        if (value == null || value.isBlank()) return "ALL";
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizeScopeValue(String value, String strategy) {
        return MetricStrategyIdentity.scopeValue(value, strategy);
    }

    private static String normalizeToken(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static int clampInt(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    private static BigDecimal errorRatePct(long events, long errors) {
        if (events <= 0) return ZERO;
        return BigDecimal.valueOf(Math.max(0L, errors))
                .multiply(HUNDRED)
                .divide(BigDecimal.valueOf(events), 6, RoundingMode.HALF_UP);
    }

    private static BigDecimal nullToZero(BigDecimal value) {
        return value == null ? ZERO : value;
    }

    private static String safe(String value) {
        if (value == null) return null;
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim();
        return clean.length() > 300 ? clean.substring(0, 300) : clean;
    }

    private void inTransaction(Runnable work) {
        transactionOperations.executeWithoutResult(status -> work.run());
    }

    private <T> T inTransaction(Supplier<T> work) {
        return transactionOperations.execute(status -> work.get());
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values) {
        if (values == null) return null;
        for (T value : values) {
            if (value != null) return value;
        }
        return null;
    }

    private record LiveDecision(
            boolean allowed,
            String reasonCode,
            Map<String, Object> details,
            long events,
            long errors,
            BigDecimal errorRatePct,
            BigDecimal netPnlUsd,
            long days,
            String resolvedCopyMode,
            LiveAllocationPercentageResolution percentageResolution
    ) {
        private LiveDecision(
                boolean allowed,
                String reasonCode,
                Map<String, Object> details,
                long events,
                long errors,
                BigDecimal errorRatePct,
                BigDecimal netPnlUsd,
                long days,
                String resolvedCopyMode
        ) {
            this(allowed, reasonCode, details, events, errors, errorRatePct, netPnlUsd, days,
                    resolvedCopyMode, null);
        }

        private LiveDecision {
            reasonCode = reasonCode == null ? "UNKNOWN" : reasonCode.trim().toUpperCase(Locale.ROOT).replace('-', '_');
            errorRatePct = nullToZero(errorRatePct);
            netPnlUsd = nullToZero(netPnlUsd);
        }
    }

    private record PromotionMutation(
            UserCopyAllocationEntity closedMicro,
            UserCopyAllocationEntity liveAllocation,
            LiveAllocationPercentageResolution percentageResolution,
            boolean created,
            String reasonCode
    ) {
        private static PromotionMutation created(
                UserCopyAllocationEntity closedMicro,
                UserCopyAllocationEntity liveAllocation,
                LiveAllocationPercentageResolution percentageResolution
        ) {
            return new PromotionMutation(closedMicro, liveAllocation, percentageResolution, true,
                    "LIVE_ALLOCATION_PCT_RESOLVED");
        }

        private static PromotionMutation noop(
                UserCopyAllocationEntity micro,
                UserCopyAllocationEntity liveAllocation
        ) {
            return new PromotionMutation(micro, liveAllocation, null, false,
                    "LIVE_ALLOCATION_ALREADY_EXISTS");
        }

        private static PromotionMutation rejected(UserCopyAllocationEntity micro, String reasonCode) {
            return new PromotionMutation(micro, null, null, false, reasonCode);
        }
    }

    private record FullDecisionGate(
            boolean allowed,
            String reasonCode
    ) {
    }
}
