package com.apunto.engine.service.impl;

import com.apunto.engine.shared.metric.MetricStrategyIdentity;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.entity.CopyPromotionAuditEntity;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import com.apunto.engine.entity.ShadowWalletProfileValidationEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.entity.UserWalletCopyPlanEntity;
import com.apunto.engine.repository.CopyPromotionAuditRepository;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.ShadowCopyAllocationRepository;
import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowPositionStateRepository;
import com.apunto.engine.repository.ShadowWalletProfileValidationRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.repository.UserRepository;
import com.apunto.engine.repository.UserWalletCopyPlanRepository;
import com.apunto.engine.repository.UserWalletCopyPreferenceRepository;
import com.apunto.engine.service.ShadowPromotionService;
import com.apunto.engine.service.copy.coverage.ShadowCoverageBatch;
import com.apunto.engine.service.copy.coverage.ShadowCoverageCalculator;
import com.apunto.engine.service.copy.coverage.ShadowCoverageDecision;
import com.apunto.engine.service.copy.coverage.ShadowCoverageQueryService;
import com.apunto.engine.service.copy.coverage.ShadowCoverageSnapshot;
import com.apunto.engine.service.copy.coverage.ShadowCoverageSource;
import com.apunto.engine.service.copy.coverage.ShadowCoverageWindowProperties;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageRequest;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolution;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolver;
import com.apunto.engine.service.copy.decision.CopyDecisionGateway;
import com.apunto.engine.service.copy.decision.CopyDecisionRequest;
import com.apunto.engine.service.copy.distribution.CopyDistributionUnitExecutor;
import com.apunto.engine.service.copy.distribution.CopyDistributionUnitExecutor.UnitMutationResult;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver.CopyModeResolution;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;
import com.apunto.engine.service.copy.account.MicroLiveCapacityDecision;
import com.apunto.engine.service.copy.account.MicroLiveCapacityGate;
import com.apunto.engine.service.copy.promotion.ShadowPromotionProperties;
import com.apunto.engine.service.copy.promotion.ShadowPromotionResult;
import com.apunto.engine.service.copy.symbol.CopySymbolResolution;
import com.apunto.engine.service.copy.symbol.CopySymbolResolver;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class ShadowPromotionServiceImpl implements ShadowPromotionService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal HUNDRED = new BigDecimal("100");
    private static final String SHADOW_STATUS_VALIDATED = "SHADOW_VALIDATED";
    private static final String SHADOW_STATUS_PROMOTED_TO_LIVE = "PROMOTED_TO_LIVE";
    private static final String PROMOTED_TO_MICRO_REASON = "PROMOTED_TO_MICRO_LIVE_RECORDED_AS_REASON";

    private final ShadowCopyAllocationRepository shadowAllocationRepository;
    private final ShadowWalletProfileValidationRepository validationRepository;
    private final ShadowCopyOperationEventRepository shadowEventRepository;
    private final ShadowPositionStateRepository shadowPositionStateRepository;
    private final UserRepository userRepository;
    private final DetailUserRepository detailUserRepository;
    private final UserApiKeyRepository userApiKeyRepository;
    private final UserWalletCopyPlanRepository planRepository;
    private final UserCopyAllocationRepository allocationRepository;
    private final CopyPromotionAuditRepository auditRepository;
    private final CopySymbolResolver copySymbolResolver;
    private final ShadowPromotionProperties properties;
    private final CopyDecisionGateway copyDecisionGateway;
    private final ShadowCoverageQueryService shadowCoverageQueryService;
    private final ShadowCoverageCalculator shadowCoverageCalculator;
    private final ShadowCoverageWindowProperties shadowCoverageProperties;

    @Autowired(required = false)
    private CopyDistributionUnitExecutor copyDistributionUnitExecutor;

    @Autowired(required = false)
    private LiveAllocationPercentageResolver liveAllocationPercentageResolver;

    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    @Autowired(required = false)
    private UserWalletCopyPreferenceRepository walletPreferenceRepository;

    @Autowired(required = false)
    private MicroLiveCapacityGate microLiveCapacityGate;

    @Autowired(required = false)
    private com.apunto.engine.service.copy.account.MicroLiveCapacityProperties microLiveCapacityProperties;

    @Override
    public ShadowPromotionResult promoteShadowToMicroLive() {
        long startedNs = System.nanoTime();
        if (properties == null || !properties.isEnabled() || !properties.isFromShadowEnabled()) {
            log.info("event=copy.promotion.shadow_to_micro.skipped reason=PROMOTION_DISABLED");
            return ShadowPromotionResult.empty();
        }

        int limit = Math.max(1, properties.getCandidateLimit());
        List<ShadowCopyAllocationEntity> candidates = shadowAllocationRepository.findPromotionCandidates(limit);
        if (candidates == null || candidates.isEmpty()) {
            log.info("event=copy.promotion.shadow_to_micro.started candidates=0");
            return ShadowPromotionResult.empty();
        }

        int evaluated = 0;
        int ready = 0;
        int created = 0;
        int rejected = 0;
        int skipped = 0;

        log.info("event=copy.promotion.shadow_to_micro.started candidates={}", candidates.size());
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        ShadowCoverageBatch coverageBatch = loadCoverageBatch(candidates, now);

        for (ShadowCopyAllocationEntity shadow : candidates) {
            long candidateNs = System.nanoTime();
            if (shadow == null || shadow.getId() == null) {
                skipped++;
                continue;
            }
            evaluated++;
            try {
                PromotionDecision decision = evaluate(shadow, now, coverageBatch);
                audit(shadow, decision, null, "SHADOW_EVALUATED");
                log.info(
                        "event=copy.promotion.shadow_candidate.evaluated userId={} walletId={} shadowAllocationId={} strategy={} sourceSymbol={} targetSymbol={} executionMode=SHADOW decision={} reasonCode={} readinessPct={} elapsedMs={}",
                        shadow.getIdUser(),
                        shadow.getWalletId(),
                        shadow.getId(),
                        shadow.getCopyStrategyCode(),
                        decision.symbolResolution() == null ? sourceSymbol(shadow) : decision.symbolResolution().sourceSymbol(),
                        decision.symbolResolution() == null ? null : decision.symbolResolution().targetSymbol(),
                        decision.allowed() ? "ALLOW_MICRO_LIVE" : "REJECT",
                        decision.reasonCode(),
                        decision.allowed() ? 100 : 0,
                        elapsedMs(candidateNs)
                );
                if (!decision.allowed() && "ALREADY_PROMOTED".equals(decision.reasonCode())) {
                    skipped++;
                    Optional<UserCopyAllocationEntity> existing = existingAllocation(shadow);
                    existing.ifPresent(allocation -> linkShadowToExistingUnderProfileLock(shadow, allocation, now));
                    audit(shadow, decision, existing.orElse(null), "SHADOW_PROMOTION_NOOP");
                    log.info(
                            "event=copy.promotion.micro_live.noop userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=MICRO_LIVE decision=NOOP reasonCode={} elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            safe(shadow.getCopyMode()),
                            existing.map(UserCopyAllocationEntity::getCopyMode).orElse(decision.resolvedCopyMode()),
                            decision.reasonCode(),
                            elapsedMs(candidateNs)
                    );
                    log.info(
                            "event=shadow.promotion.micro_live.noop_existing userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} strategyCode={} scopeType={} scopeValue={} reasonCode={} elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            existing.map(UserCopyAllocationEntity::getId).orElse(null),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            decision.reasonCode(),
                            elapsedMs(candidateNs)
                    );
                    log.info(
                            "event=copy.promotion.shadow_to_micro.noop userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=MICRO_LIVE decision=NOOP reasonCode={} elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            safe(shadow.getCopyMode()),
                            existing.map(UserCopyAllocationEntity::getCopyMode).orElse(decision.resolvedCopyMode()),
                            decision.reasonCode(),
                            elapsedMs(candidateNs)
                    );
                    continue;
                }
                if (!decision.allowed()) {
                    if (!markRejectedUnderProfileLock(shadow, decision, now)) {
                        skipped++;
                        log.info("event=copy.promotion.shadow_rejection.noop userId={} walletId={} shadowAllocationId={} strategyCode={} decision=NOOP reasonCode=ALREADY_PROMOTED profileLockResolvedRace=true elapsedMs={}",
                                shadow.getIdUser(), shadow.getWalletId(), shadow.getId(), shadow.getCopyStrategyCode(), elapsedMs(candidateNs));
                        continue;
                    }
                    rejected++;
                    audit(shadow, decision, null, "SHADOW_PROMOTION_REJECTED");
                    boolean directLiveRejected = isDirectLiveReason(decision.reasonCode());
                    if (isLivePercentageReason(decision.reasonCode())) {
                        recordLivePromotion("rejected", decision.reasonCode());
                        log.info("event=copy.promotion.live.rejected executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} previousExecutionMode=SHADOW previousAllocationPct={} resolvedLiveAllocationPct=null allocationPctSource=null decision=KEEP_SHADOW reasonCode={} retryable=true shouldAlert=false recommendedAction=WAIT_FOR_NEXT_VALID_DISTRIBUTION",
                                shadow.getIdUser(), shadow.getWalletId(), shadow.getCopyStrategyCode(),
                                shadow.getScopeType(), shadow.getScopeValue(), shadow.getAllocationPct(),
                                decision.reasonCode());
                    }
                    log.info(
                            "event={} userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=SHADOW decision=REJECT reasonCode={} elapsedMs={} details={}",
                            directLiveRejected ? "copy.promotion.shadow_to_live.rejected" : "copy.promotion.shadow_to_micro.rejected",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            safe(shadow.getCopyMode()),
                            decision.resolvedCopyMode(),
                            decision.reasonCode(),
                            elapsedMs(candidateNs),
                            decision.details()
                    );
                    continue;
                }
                PromotionUnitOutcome promotion = promoteUnderProfileLock(shadow, decision, now);
                if (!promotion.created()) {
                    skipped++;
                    UserCopyAllocationEntity existing = promotion.output().allocation();
                    PromotionDecision noop = PromotionDecision.rejected("ALREADY_PROMOTED", details(shadow, null, extras(
                            "existingAllocationId", existing == null ? null : existing.getId(),
                            "raceResolvedByProfileLock", true
                    )));
                    audit(shadow, noop, existing, "SHADOW_PROMOTION_NOOP");
                    log.info("event=copy.promotion.shadow_to_micro.noop userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} strategyCode={} scopeType={} scopeValue={} executionMode={} decision=NOOP reasonCode=ALREADY_PROMOTED profileLockResolvedRace=true elapsedMs={}",
                            shadow.getIdUser(), shadow.getWalletId(), shadow.getId(), existing == null ? null : existing.getId(),
                            shadow.getCopyStrategyCode(), shadow.getScopeType(), shadow.getScopeValue(),
                            existing == null ? decision.targetExecutionMode() : existing.getExecutionMode(), elapsedMs(candidateNs));
                    continue;
                }
                ready++;
                PromotionOutput output = promotion.output();
                created++;
                boolean liveCreated = "LIVE".equals(output.allocation().getExecutionMode());
                audit(shadow, decision, output.allocation(), liveCreated ? "LIVE_ALLOCATION_CREATED" : "MICRO_LIVE_CREATED");
                log.info(
                        "event={} userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} planId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} sourceSymbol={} targetSymbol={} executionMode={} decision=CREATED reasonCode={} capital={} elapsedMs={}",
                        liveCreated ? "copy.promotion.shadow_to_live.created" : "copy.promotion.micro_live.created",
                        shadow.getIdUser(),
                        shadow.getWalletId(),
                        shadow.getId(),
                        output.allocation().getId(),
                        output.plan().getId(),
                        shadow.getCopyStrategyCode(),
                        shadow.getScopeType(),
                        shadow.getScopeValue(),
                        safe(shadow.getCopyMode()),
                        output.allocation().getCopyMode(),
                        decision.symbolResolution() == null ? sourceSymbol(shadow) : decision.symbolResolution().sourceSymbol(),
                        decision.symbolResolution() == null ? null : decision.symbolResolution().targetSymbol(),
                        output.allocation().getExecutionMode(),
                        decision.reasonCode(),
                        decision.targetCapital(),
                        elapsedMs(candidateNs)
                );
                if (!liveCreated) {
                    log.info(
                            "event=shadow.promotion.micro_live.created userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} strategyCode={} scopeType={} scopeValue={} reasonCode={} elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            output.allocation().getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            decision.reasonCode(),
                            elapsedMs(candidateNs)
                    );
                    log.info(
                            "event=copy.promotion.shadow_to_micro.created userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} planId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} sourceSymbol={} targetSymbol={} executionMode=MICRO_LIVE decision=CREATED reasonCode={} capital={} elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            output.allocation().getId(),
                            output.plan().getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            safe(shadow.getCopyMode()),
                            output.allocation().getCopyMode(),
                            decision.symbolResolution() == null ? sourceSymbol(shadow) : decision.symbolResolution().sourceSymbol(),
                            decision.symbolResolution() == null ? null : decision.symbolResolution().targetSymbol(),
                            decision.reasonCode(),
                            decision.targetCapital(),
                            elapsedMs(candidateNs)
                    );
                }
            } catch (LiveAllocationPercentageRejectedException ex) {
                rejected++;
                PromotionDecision failed = PromotionDecision.rejected(ex.reasonCode(), details(shadow, null, extras(
                        "liveAllocationReasonCode", ex.reasonCode(),
                        "targetExecutionMode", "LIVE"
                )));
                markRejectedUnderProfileLock(shadow, failed, now);
                recordLivePromotion("rejected", ex.reasonCode());
                log.info("event=copy.promotion.live.rejected executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} previousExecutionMode=SHADOW previousAllocationPct={} resolvedLiveAllocationPct=null allocationPctSource=null decision=KEEP_SHADOW reasonCode={} retryable=true shouldAlert=false recommendedAction=WAIT_FOR_NEXT_VALID_DISTRIBUTION",
                        shadow.getIdUser(), shadow.getWalletId(), shadow.getCopyStrategyCode(), shadow.getScopeType(),
                        shadow.getScopeValue(), shadow.getAllocationPct(), ex.reasonCode());
            } catch (DataIntegrityViolationException ex) {
                Optional<UserCopyAllocationEntity> existing = existingAllocation(shadow);
                if (existing.isPresent()) {
                    skipped++;
                    PromotionDecision noop = PromotionDecision.rejected("ALREADY_PROMOTED", details(shadow, null, extras(
                            "existingAllocationId", existing.get().getId(),
                            "errorClass", ex.getClass().getSimpleName()
                    )));
                    linkShadowToExistingUnderProfileLock(shadow, existing.get(), now);
                    audit(shadow, noop, existing.get(), "SHADOW_PROMOTION_NOOP");
                    log.info(
                            "event=copy.promotion.micro_live.noop userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=MICRO_LIVE decision=NOOP reasonCode=ALREADY_PROMOTED elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            existing.get().getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            safe(shadow.getCopyMode()),
                            existing.get().getCopyMode(),
                            elapsedMs(candidateNs)
                    );
                    log.info(
                            "event=shadow.promotion.micro_live.noop_existing userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} strategyCode={} scopeType={} scopeValue={} reasonCode=ALREADY_PROMOTED elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            existing.get().getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            elapsedMs(candidateNs)
                    );
                    log.info(
                            "event=copy.promotion.shadow_to_micro.noop userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=MICRO_LIVE decision=NOOP reasonCode=ALREADY_PROMOTED elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            existing.get().getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            safe(shadow.getCopyMode()),
                            existing.get().getCopyMode(),
                            elapsedMs(candidateNs)
                    );
                    continue;
                }
                rejected++;
                String integrityReason = containsReason(ex, "MICRO_LIVE_CAPACITY_EXHAUSTED")
                        ? "MICRO_LIVE_SLOT_CONCURRENCY_LOST"
                        : "PROMOTION_FAILED_DUPLICATE_CONSTRAINT";
                PromotionDecision failed = PromotionDecision.rejected(integrityReason, extras(
                        "errorClass", ex.getClass().getSimpleName(),
                        "error", safe(ex.getMessage())
                ));
                audit(shadow, failed, null, "SHADOW_PROMOTION_REJECTED");
                log.warn(
                        "event=copy.promotion.shadow_to_micro.candidate_failed userId={} walletId={} shadowAllocationId={} strategy={} reasonCode={} errorClass={} errorMessage=\"{}\" elapsedMs={}",
                        shadow.getIdUser(),
                        shadow.getWalletId(),
                        shadow.getId(),
                        shadow.getCopyStrategyCode(),
                        failed.reasonCode(),
                        ex.getClass().getSimpleName(),
                        safe(ex.getMessage()),
                        elapsedMs(candidateNs)
                );
            } catch (ExecutionAccountUnavailableException ex) {
                rejected++;
                PromotionDecision failed = PromotionDecision.rejected(ex.reasonCode(), extras(
                        "targetExecutionMode", ex.executionMode()));
                markRejectedUnderProfileLock(shadow, failed, now);
                audit(shadow, failed, null, "SHADOW_PROMOTION_REJECTED");
                log.info("event=copy.promotion.shadow_to_micro.rejected userId={} walletId={} shadowAllocationId={} executionMode={} decision=REJECT reasonCode={}",
                        shadow.getIdUser(), shadow.getWalletId(), shadow.getId(), ex.executionMode(), ex.reasonCode());
            } catch (RuntimeException ex) {
                rejected++;
                PromotionDecision failed = PromotionDecision.rejected("PROMOTION_FAILED", extras(
                        "errorClass", ex.getClass().getSimpleName(),
                        "error", safe(ex.getMessage())
                ));
                audit(shadow, failed, null, "SHADOW_PROMOTION_REJECTED");
                log.warn(
                        "event=copy.promotion.shadow_to_micro.candidate_failed userId={} walletId={} shadowAllocationId={} strategy={} reasonCode={} errorClass={} errorMessage=\"{}\" elapsedMs={}",
                        shadow.getIdUser(),
                        shadow.getWalletId(),
                        shadow.getId(),
                        shadow.getCopyStrategyCode(),
                        failed.reasonCode(),
                        ex.getClass().getSimpleName(),
                        safe(ex.getMessage()),
                        elapsedMs(candidateNs)
                );
            }
        }

        log.info(
                "event=copy.promotion.shadow_to_micro.finished evaluated={} ready={} promotedCount={} rejectedCount={} skippedCount={} elapsedMs={}",
                evaluated, ready, created, rejected, skipped
                , elapsedMs(startedNs)
        );
        return new ShadowPromotionResult(evaluated, ready, created, rejected, skipped);
    }

    private ShadowCoverageBatch loadCoverageBatch(
            List<ShadowCopyAllocationEntity> candidates,
            OffsetDateTime now
    ) {
        List<Long> allocationIds = candidates.stream()
                .filter(Objects::nonNull)
                .map(ShadowCopyAllocationEntity::getId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        try {
            ShadowCoverageBatch batch = shadowCoverageQueryService.load(allocationIds, now);
            if (batch != null) return batch;
        } catch (RuntimeException ex) {
            log.warn(
                    "event=shadow.coverage.query_failed allocationCount={} reasonCode=SHADOW_COVERAGE_ROLLING_QUERY_FAILED errorClass={} errorMessage=\"{}\"",
                    allocationIds.size(),
                    ex.getClass().getSimpleName(),
                    safe(ex.getMessage())
            );
            return ShadowCoverageBatch.failure(
                    allocationIds,
                    now.minusDays(shadowCoverageProperties.getWindowDays()),
                    now,
                    ex
            );
        }
        IllegalStateException failure = new IllegalStateException("shadow coverage query returned null");
        return ShadowCoverageBatch.failure(
                allocationIds,
                now.minusDays(shadowCoverageProperties.getWindowDays()),
                now,
                failure
        );
    }

    private PromotionDecision evaluate(
            ShadowCopyAllocationEntity shadow,
            OffsetDateTime now,
            ShadowCoverageBatch coverageBatch
    ) {
        if (shadow.getLinkedLiveAllocationId() != null) {
            return rejected("ALREADY_PROMOTED", shadow);
        }
        if (shadow.getIdUser() == null) {
            return rejected("NO_ACTIVE_USER", shadow);
        }
        Optional<UserCopyAllocationEntity> existing = existingAllocation(shadow);
        if (existing.isPresent()) {
            return PromotionDecision.rejected("ALREADY_PROMOTED", details(shadow, null, extras(
                    "microLiveAllocationReasonCode", "MICRO_LIVE_ALLOCATION_ALREADY_EXISTS",
                    "existingAllocationId", existing.get().getId()
            )));
        }

        UserEntity user = userRepository.findById(shadow.getIdUser()).orElse(null);
        if (user == null || !user.isActivo()) {
            return rejected("NO_ACTIVE_USER", shadow);
        }

        DetailUserEntity detail = detailUserRepository.findByUser_Id(shadow.getIdUser());
        if (detail == null || !detail.isUserActive()) {
            return rejected("NO_ACTIVE_USER", shadow);
        }
        if (!detail.isParticipateInMicroLive()) {
            return rejected("MICRO_LIVE_USER_OPT_IN_REQUIRED", shadow);
        }
        if (walletPreferenceRepository != null
                && walletPreferenceRepository.isBlocked(shadow.getIdUser(), shadow.getWalletId())) {
            return rejected("COPY_WALLET_BLOCKED_BY_USER", shadow);
        }
        if (!detail.isApiKeyBinar()) {
            return rejected("NO_ACTIVE_BINANCE_API_KEY", shadow);
        }

        CapitalConfig capitalConfig = resolveCapitalConfig(detail);
        log.info(
                "event=copy.promotion.capital_config.resolved userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} reasonCode={} executionMode=SHADOW decision={} capital={} capitalAsset={} maxWallet={}",
                shadow.getIdUser(),
                shadow.getWalletId(),
                shadow.getId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                capitalConfig.reasonCode(),
                capitalConfig.valid() ? "ALLOW" : "REJECT",
                capitalConfig.capital(),
                safe(capitalConfig.capitalAsset()),
                capitalConfig.maxWallet()
        );
        if (!capitalConfig.valid()) {
            return PromotionDecision.rejected("NO_CAPITAL_CONFIG", details(shadow, null, extras(
                    "capitalConfigReasonCode", "CAPITAL_CONFIG_MISSING_FROM_USER_DETAIL",
                    "capitalConfigFailure", capitalConfig.reasonCode()
            )));
        }

        BigDecimal requiredMicroCapital = positiveOrDefault(
                properties.getMicroLiveInitialCapitalUsd(),
                new BigDecimal("100")
        ).setScale(8, RoundingMode.HALF_UP);
        BigDecimal availableCapital = BigDecimal.valueOf(capitalConfig.capital()).setScale(8, RoundingMode.HALF_UP);
        if (availableCapital.compareTo(requiredMicroCapital) < 0) {
            return PromotionDecision.rejected(
                    "MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE",
                    details(shadow, null, extras(
                            "availableCapitalUsd", availableCapital,
                            "requiredMicroLiveCapitalUsd", requiredMicroCapital
                    ))
            );
        }

        long activeExecutionAllocations = allocationRepository.countActiveExecutionAllocationsByUser(shadow.getIdUser());
        if (activeExecutionAllocations >= capitalConfig.maxWallet()) {
            return PromotionDecision.rejected("MAX_WALLET_REACHED", details(shadow, null, extras(
                    "activeExecutionAllocations", activeExecutionAllocations,
                    "maxWallet", capitalConfig.maxWallet()
            )));
        }

        ShadowWalletProfileValidationEntity validation = shadow.getWalletProfileId() == null
                ? null
                : validationRepository.findFirstByWalletProfileIdOrderByStartedAtDesc(shadow.getWalletProfileId()).orElse(null);
        Evidence evidence = evidence(shadow, validation, now, coverageBatch);
        PromotionDecision readiness = evaluateEvidence(shadow, evidence, now);
        if (!readiness.allowed()) {
            return readiness;
        }

        PromotionDecision fullDecision = evaluateFullDecisionForMicroLive(shadow, evidence);
        if (!fullDecision.allowed()) {
            return fullDecision;
        }

        CopyModeResolution copyModeResolution = resolveUserCopyAllocationCopyMode(shadow);
        logCopyModeResolved(shadow, copyModeResolution, copyModeResolution.valid() ? "ALLOW" : "REJECT");
        if (!copyModeResolution.valid()) {
            return PromotionDecision.rejected(UserCopyAllocationCopyModeResolver.INVALID_COPY_MODE_MAPPING, details(shadow, evidence, extras(
                    "sourceCopyMode", shadow.getCopyMode(),
                    "resolvedCopyMode", copyModeResolution.copyMode(),
                    "copyModeReasonCode", copyModeResolution.reasonCode(),
                    "copyModeNormalizedStrategyCode", copyModeResolution.normalizedStrategyCode(),
                    "copyModeNormalizedSourceCopyMode", copyModeResolution.normalizedSourceCopyMode()
            )));
        }

        CopySymbolResolution symbolResolution;
        try {
            symbolResolution = resolveSymbol(shadow, capitalConfig.capitalAsset());
        } catch (RuntimeException ex) {
            return PromotionDecision.rejected("SYMBOL_RESOLVER_FAILED", details(shadow, evidence, extras(
                    "sourceSymbol", sourceSymbol(shadow),
                    "capitalAsset", capitalConfig.capitalAsset(),
                    "errorClass", ex.getClass().getSimpleName(),
                    "error", safe(ex.getMessage())
            )));
        }
        if (symbolResolution != null && !symbolResolution.resolved()) {
            return PromotionDecision.rejected(
                    firstNonBlank(symbolResolution.reasonCode(), "SHADOW_NOT_READY_SYMBOL_UNRESOLVED"),
                    details(shadow, evidence, extras(
                            "sourceSymbol", symbolResolution.sourceSymbol(),
                            "targetSymbol", symbolResolution.targetSymbol(),
                            "capitalAsset", symbolResolution.capitalAsset()
                    ))
            );
        }

        BigDecimal microCapital = requiredMicroCapital;
        DirectLiveDecision targetDecision = directLiveDecision(shadow);
        logDirectLivePolicyChecked(shadow, targetDecision);
        if (!targetDecision.allowed()) {
            return PromotionDecision.rejected(targetDecision.reasonCode(), details(shadow, evidence, extras(
                    "targetExecutionMode", targetDecision.targetExecutionMode(),
                    "directLivePolicy", targetDecision.policy(),
                    "directLiveReasonCode", targetDecision.reasonCode(),
                    "sourceCopyMode", shadow.getCopyMode(),
                    "resolvedCopyMode", copyModeResolution.copyMode(),
                    "copyModeReasonCode", copyModeResolution.reasonCode(),
                    "copyModeConstraintReasonCode", copyModeResolution.constraintReasonCode()
            )));
        }
        ExecutionAccountPurpose accountPurpose = "LIVE".equals(targetDecision.targetExecutionMode())
                ? ExecutionAccountPurpose.LIVE : ExecutionAccountPurpose.MICRO_LIVE;
        UserApiKeyEntity account = executionAccount(shadow.getIdUser(), accountPurpose);
        if (!usable(account)) {
            return rejected(accountPurpose == ExecutionAccountPurpose.MICRO_LIVE
                    ? "MICRO_LIVE_EXECUTION_ACCOUNT_MISSING"
                    : "LIVE_EXECUTION_ACCOUNT_MISSING", shadow);
        }
        if (accountPurpose == ExecutionAccountPurpose.MICRO_LIVE && microLiveCapacityGate != null) {
            MicroLiveCapacityDecision capacity = microLiveCapacityGate.evaluate(
                    account, shadow.getIdUser(), capitalConfig.capitalAsset());
            if (!capacity.allowed()) {
                return PromotionDecision.rejected(capacity.reasonCode(), details(shadow, evidence, extras(
                        "exchangeAccountId", account.getId(),
                        "occupiedMicroLiveSlots", capacity.occupiedSlots(),
                        "effectiveMicroLiveMaxSlots", capacity.effectiveMaxSlots(),
                        "walletBalance", capacity.walletBalance(),
                        "availableBalance", capacity.availableBalance(),
                        "requiredReservedCapital", capacity.requiredReservedCapital()
                )));
            }
        }
        LiveAllocationPercentageResolution percentageResolution = null;
        if ("LIVE".equals(targetDecision.targetExecutionMode())) {
            percentageResolution = resolveLivePercentage(shadow, now);
            String reasonCode = livePercentageFailureReason(percentageResolution, now);
            if (reasonCode != null) {
                return PromotionDecision.rejected(reasonCode, details(shadow, evidence, extras(
                        "targetExecutionMode", "LIVE",
                        "resolvedLiveAllocationPct", percentageResolution == null ? null : percentageResolution.percentage(),
                        "allocationPctSource", percentageResolution == null ? null : percentageResolution.source(),
                        "liveAllocationReasonCode", reasonCode
                )));
            }
        }
        BigDecimal targetCapital = "LIVE".equals(targetDecision.targetExecutionMode())
                ? BigDecimal.valueOf(capitalConfig.capital())
                .multiply(percentageResolution.walletTotalPercentage())
                .setScale(8, RoundingMode.HALF_UP)
                : microCapital;
        Map<String, Object> finalDetails = extras(
                "capital", capitalConfig.capital(),
                "maxWallet", capitalConfig.maxWallet(),
                "microLiveCapital", microCapital,
                "targetCapital", targetCapital,
                "targetExecutionMode", targetDecision.targetExecutionMode(),
                "directLivePolicy", targetDecision.policy(),
                "directLiveReasonCode", targetDecision.reasonCode(),
                "capitalAsset", capitalConfig.capitalAsset(),
                "capitalConfigReasonCode", "CAPITAL_CONFIG_FOUND_FROM_USER_DETAIL",
                "sourceCopyMode", shadow.getCopyMode(),
                "resolvedCopyMode", copyModeResolution.copyMode(),
                "copyModeReasonCode", copyModeResolution.reasonCode(),
                "copyModeConstraintReasonCode", copyModeResolution.constraintReasonCode(),
                "sourceSymbol", symbolResolution == null ? null : symbolResolution.sourceSymbol(),
                "targetSymbol", symbolResolution == null ? null : symbolResolution.targetSymbol()
        );
        if (percentageResolution != null) {
            finalDetails.put("resolvedLiveAllocationPct", percentageResolution.percentage());
            finalDetails.put("walletTotalAllocationPct", percentageResolution.walletTotalPercentage());
            finalDetails.put("allocationPctSource", percentageResolution.source());
            finalDetails.put("distributionDecisionId", percentageResolution.sourceId());
            finalDetails.put("distributionCalculatedAt", percentageResolution.calculatedAt());
            finalDetails.put("distributionValidUntil", percentageResolution.validUntil());
        }
        finalDetails.putAll(fullDecision.details());
        return PromotionDecision.allowed(details(shadow, evidence, finalDetails), detail, evidence,
                symbolResolution, microCapital, copyModeResolution.copyMode(),
                targetDecision.targetExecutionMode(), targetCapital, percentageResolution);
    }

    private PromotionDecision evaluateEvidence(ShadowCopyAllocationEntity shadow, Evidence evidence, OffsetDateTime now) {
        boolean summaryOnlyBlockedBySummary = summaryOnlyBlockedBySummary(shadow);
        logCoverageEvaluated(shadow, evidence.coverageSnapshot());
        log.info(
                "event=shadow.promotion.evidence.checked userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} shadowDays={} events={} closedPositions={} historicalCoveragePct={} rollingCoveragePct={} coverageSourceUsed={} coverageDecision={} coverageReasonCode={} netPnlUsd={} summaryOnlyOverride={}",
                shadow.getIdUser(),
                shadow.getWalletId(),
                shadow.getId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                evidence.shadowDays(),
                evidence.events(),
                evidence.closedPositions(),
                evidence.coveragePct(),
                evidence.coverageSnapshot().rollingCoveragePct(),
                evidence.coverageSnapshot().coverageSourceUsed(),
                evidence.coverageSnapshot().coverageDecision(),
                evidence.coverageSnapshot().coverageReasonCode(),
                evidence.netPnlUsd(),
                summaryOnlyBlockedBySummary
        );
        if (properties.isRequireCopyGuardOpen() && !copyGuardOpen(shadow) && !summaryOnlyBlockedBySummary) {
            return PromotionDecision.rejected("SHADOW_NOT_READY_COPY_GUARD", details(shadow, evidence, Map.of()));
        }
        if (properties.isRequireActiveWallet() && inactive(shadow, now)) {
            return PromotionDecision.rejected("SHADOW_NOT_READY_INACTIVE", details(shadow, evidence, Map.of()));
        }
        if (evidence.shadowDays() < Math.max(0, properties.getMinShadowDays())) {
            return PromotionDecision.rejected("SHADOW_NOT_READY_MIN_DAYS", details(shadow, evidence, Map.of()));
        }
        if (evidence.events() < Math.max(0, properties.getMinShadowEvents())) {
            return PromotionDecision.rejected("SHADOW_NOT_READY_MIN_EVENTS", details(shadow, evidence, Map.of()));
        }
        if (evidence.closedPositions() < Math.max(0, properties.getMinShadowClosedPositions())) {
            return PromotionDecision.rejected("SHADOW_NOT_READY_MIN_CLOSED_POSITIONS", details(shadow, evidence, Map.of()));
        }
        ShadowCoverageSnapshot coverage = evidence.coverageSnapshot();
        if (coverage.coverageSourceUsed() == ShadowCoverageSource.ROLLING) {
            if (coverage.coverageDecision() != ShadowCoverageDecision.COVERAGE_READY) {
                logCoverageBlocked(shadow, coverage);
                return PromotionDecision.rejected(
                        coverage.coverageReasonCode().name(),
                        details(shadow, evidence, Map.of())
                );
            }
        } else {
            BigDecimal minCoverage = nullToZero(properties.getMinShadowCoveragePct());
            if (minCoverage.compareTo(ZERO) > 0 && evidence.coveragePct().compareTo(minCoverage) < 0) {
                logCoverageBlocked(shadow, coverage);
                return PromotionDecision.rejected("SHADOW_NOT_READY_COVERAGE", details(shadow, evidence, Map.of()));
            }
        }
        log.info(
                "event=shadow.promotion.coverage_ready shadowAllocationId={} walletId={} strategyCode={} scopeType={} scopeValue={} historicalCoveragePct={} rollingCoveragePct={} rollingEvaluable={} coverageSourceUsed={} coverageDecision={} reasonCode={} decision=CONTINUE_OTHER_GATES",
                shadow.getId(),
                shadow.getWalletId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                coverage.historicalCoveragePct(),
                coverage.rollingCoveragePct(),
                coverage.rollingEvaluableEvents(),
                coverage.coverageSourceUsed(),
                coverage.coverageDecision(),
                coverage.coverageReasonCode()
        );
        if (properties.isRequirePositiveShadowPnl()
                && evidence.netPnlUsd().compareTo(nullToZero(properties.getMinShadowNetPnlUsd())) < 0) {
            return PromotionDecision.rejected("SHADOW_NOT_READY_NEGATIVE_PNL", details(shadow, evidence, Map.of()));
        }
        BigDecimal maxDrawdown = properties.getMaxShadowDrawdownPct();
        if (maxDrawdown != null && maxDrawdown.compareTo(ZERO) > 0
                && evidence.maxDrawdownPct().abs().compareTo(maxDrawdown) > 0) {
            return PromotionDecision.rejected("SHADOW_NOT_READY_DRAWDOWN", details(shadow, evidence, Map.of()));
        }
        return PromotionDecision.allowed(details(shadow, evidence, summaryOnlyBlockedBySummary
                ? extras(
                "previousLastValidationReason", "SUMMARY_NOT_FINAL_LIVE_BLOCKED",
                "shadowEvidenceOverride", true
        )
                : Map.of()), null, evidence, null, ZERO);
    }

    private PromotionDecision evaluateFullDecisionForMicroLive(ShadowCopyAllocationEntity shadow, Evidence evidence) {
        if (!properties.isRequireFullDecisionForMicroLive()) {
            return PromotionDecision.allowed(details(shadow, evidence, extras(
                    "fullDecisionRequired", false,
                    "fullDecisionReasonCode", "FULL_DECISION_REQUIREMENT_DISABLED"
            )), null, evidence, null, ZERO);
        }
        CopyDecisionRequest request = new CopyDecisionRequest(
                normalizeWallet(shadow.getWalletId()),
                normalizeStrategy(shadow.getCopyStrategyCode()),
                normalizeScopeType(shadow.getScopeType()),
                normalizeScopeValue(shadow.getScopeValue(), shadow.getCopyStrategyCode()),
                "micro-live-entry",
                "full",
                clampInt(properties.getCopyDecisionMinHistoryDays(), 1, 3650),
                clampInt(properties.getCopyDecisionSimulationLookbackDays(), 1, 3650),
                clampInt(properties.getCopyDecisionMaxFactsPerUnit(), 1, 50000),
                clampInt(properties.getCopyDecisionTimeoutMs(), 1, 59000),
                false
        );
        log.info(
                "event=shadow.promotion.full_decision.required userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} mode={} minHistoryDays={} lookbackDays={} timeoutMs={}",
                shadow.getIdUser(),
                shadow.getWalletId(),
                shadow.getId(),
                request.strategyCode(),
                request.scopeType(),
                request.scopeValue(),
                request.mode(),
                request.minHistoryDays(),
                request.simulationLookbackDays(),
                request.timeoutMs()
        );

        CopyDecisionDto decision;
        try {
            decision = copyDecisionGateway.getFullDecisionExact(request);
        } catch (RuntimeException ex) {
            log.warn(
                    "event=shadow.promotion.full_decision.result userId={} walletId={} shadowAllocationId={} decision=REJECT reasonCode=FULL_DECISION_FAILED errorClass={} errorMessage=\"{}\"",
                    shadow.getIdUser(),
                    shadow.getWalletId(),
                    shadow.getId(),
                    ex.getClass().getSimpleName(),
                    safe(ex.getMessage())
            );
            return PromotionDecision.rejected("FULL_DECISION_FAILED", details(shadow, evidence, extras(
                    "fullDecisionErrorClass", ex.getClass().getSimpleName(),
                    "fullDecisionError", safe(ex.getMessage())
            )));
        }

        FullDecisionGate gate = fullDecisionGate(decision, true);
        log.info(
                "event=shadow.promotion.full_decision.result userId={} walletId={} shadowAllocationId={} decision={} reasonCode={} fullMaterialized={} factPayloadLoaded={} decisionFinal={} requiresFullSimulation={} copyGuardAction={} canMicroLive={} canLive={}",
                shadow.getIdUser(),
                shadow.getWalletId(),
                shadow.getId(),
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
        if (!gate.allowed()) {
            log.info(
                    "event=shadow.promotion.blocked userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} reasonCode={}",
                    shadow.getIdUser(),
                    shadow.getWalletId(),
                    shadow.getId(),
                    shadow.getCopyStrategyCode(),
                    shadow.getScopeType(),
                    shadow.getScopeValue(),
                    gate.reasonCode()
            );
            return PromotionDecision.rejected(gate.reasonCode(), details(shadow, evidence, fullDecisionDetails(decision, gate)));
        }
        return PromotionDecision.allowed(details(shadow, evidence, fullDecisionDetails(decision, gate)), null, evidence, null, ZERO);
    }

    private PromotionUnitOutcome promoteUnderProfileLock(
            ShadowCopyAllocationEntity shadow,
            PromotionDecision decision,
            OffsetDateTime now
    ) {
        if (copyDistributionUnitExecutor == null) {
            return new PromotionUnitOutcome(promote(shadow, decision, now), true);
        }
        PromotionUnitOutcome[] outcome = new PromotionUnitOutcome[1];
        copyDistributionUnitExecutor.execute(
                shadow.getIdUser(),
                shadow.getWalletId(),
                strategyKey(shadow),
                null,
                () -> {
                    ShadowCopyAllocationEntity current = shadowAllocationRepository.findById(shadow.getId())
                            .orElseThrow(() -> new IllegalStateException("SHADOW_PROMOTION_ALLOCATION_MISSING"));
                    Optional<UserCopyAllocationEntity> existing = existingAllocation(current);
                    if (existing.isEmpty() && current.getLinkedLiveAllocationId() != null) {
                        existing = allocationRepository.findById(current.getLinkedLiveAllocationId());
                    }
                    if (existing.isPresent()) {
                        UserCopyAllocationEntity allocation = existing.get();
                        linkShadowToExisting(current, allocation, now);
                        outcome[0] = new PromotionUnitOutcome(new PromotionOutput(null, allocation), false);
                        return UnitMutationResult.persisted(false, false);
                    }
                    if (current.getLinkedLiveAllocationId() != null) {
                        throw new IllegalStateException("SHADOW_PROMOTION_LINKED_ALLOCATION_MISSING");
                    }
                    PromotionOutput created = promote(current, decision, now);
                    outcome[0] = new PromotionUnitOutcome(created, true);
                    return UnitMutationResult.persisted(true, false);
                }
        );
        return Objects.requireNonNull(outcome[0], "promotion unit outcome");
    }

    private boolean markRejectedUnderProfileLock(
            ShadowCopyAllocationEntity shadow,
            PromotionDecision decision,
            OffsetDateTime now
    ) {
        if (copyDistributionUnitExecutor == null) {
            markRejected(shadow, decision, now);
            return true;
        }
        boolean[] recorded = new boolean[1];
        copyDistributionUnitExecutor.execute(
                shadow.getIdUser(),
                shadow.getWalletId(),
                strategyKey(shadow),
                shadow.getLinkedLiveAllocationId(),
                () -> {
                    ShadowCopyAllocationEntity current = shadowAllocationRepository.findById(shadow.getId())
                            .orElseThrow(() -> new IllegalStateException("SHADOW_PROMOTION_ALLOCATION_MISSING"));
                    if (current.getLinkedLiveAllocationId() != null) {
                        return UnitMutationResult.none();
                    }
                    markRejected(current, decision, now);
                    recorded[0] = true;
                    return UnitMutationResult.persisted(false, false);
                }
        );
        return recorded[0];
    }

    private void linkShadowToExistingUnderProfileLock(
            ShadowCopyAllocationEntity shadow,
            UserCopyAllocationEntity allocation,
            OffsetDateTime now
    ) {
        if (copyDistributionUnitExecutor == null) {
            linkShadowToExisting(shadow, allocation, now);
            return;
        }
        copyDistributionUnitExecutor.execute(
                shadow.getIdUser(),
                shadow.getWalletId(),
                strategyKey(shadow),
                allocation.getId(),
                () -> {
                    ShadowCopyAllocationEntity current = shadowAllocationRepository.findById(shadow.getId())
                            .orElseThrow(() -> new IllegalStateException("SHADOW_PROMOTION_ALLOCATION_MISSING"));
                    UserCopyAllocationEntity currentAllocation = allocation.getId() == null
                            ? allocation
                            : allocationRepository.findById(allocation.getId()).orElse(allocation);
                    linkShadowToExisting(current, currentAllocation, now);
                    return UnitMutationResult.persisted(false, false);
                }
        );
    }

    private PromotionOutput promote(ShadowCopyAllocationEntity shadow, PromotionDecision decision, OffsetDateTime now) {
        DetailUserEntity detail = Objects.requireNonNull(decision.detail(), "detail");
        String targetExecutionMode = decision.targetExecutionMode();
        LiveAllocationPercentageResolution percentageResolution = decision.percentageResolution();
        if ("LIVE".equals(targetExecutionMode)) {
            OffsetDateTime recheckTime = OffsetDateTime.now();
            percentageResolution = resolveLivePercentage(shadow, recheckTime);
            String reasonCode = livePercentageFailureReason(percentageResolution, recheckTime);
            if (reasonCode != null) {
                throw new LiveAllocationPercentageRejectedException(reasonCode);
            }
            decision.details().put("resolvedLiveAllocationPct", percentageResolution.percentage());
            decision.details().put("walletTotalAllocationPct", percentageResolution.walletTotalPercentage());
            decision.details().put("allocationPctSource", percentageResolution.source());
            decision.details().put("distributionDecisionId", percentageResolution.sourceId());
            decision.details().put("distributionCalculatedAt", percentageResolution.calculatedAt());
            decision.details().put("distributionValidUntil", percentageResolution.validUntil());
        }
        BigDecimal allocationPct = "LIVE".equals(targetExecutionMode)
                ? percentageResolution.percentage()
                : null;
        BigDecimal targetCapital = "LIVE".equals(targetExecutionMode)
                ? BigDecimal.valueOf(Math.max(0, detail.getCapital() == null ? 0 : detail.getCapital()))
                .multiply(percentageResolution.walletTotalPercentage())
                .setScale(8, RoundingMode.HALF_UP)
                : decision.targetCapital();

        PlanResolution planResolution = resolveCopyPlan(
                shadow, detail, decision, now, targetCapital, percentageResolution);
        UserWalletCopyPlanEntity plan = planResolution.plan();
        decision.details().put("copyPlanReasonCode", planResolution.reasonCode());
        String resolvedCopyMode = Objects.requireNonNull(decision.resolvedCopyMode(), "resolvedCopyMode");
        ExecutionAccountPurpose accountPurpose = "LIVE".equals(targetExecutionMode)
                ? ExecutionAccountPurpose.LIVE : ExecutionAccountPurpose.MICRO_LIVE;
        UserApiKeyEntity executionAccount = executionAccount(shadow.getIdUser(), accountPurpose);
        if (!usable(executionAccount)) {
            throw new ExecutionAccountUnavailableException(
                    targetExecutionMode,
                    accountPurpose == ExecutionAccountPurpose.MICRO_LIVE
                            ? "MICRO_LIVE_EXECUTION_ACCOUNT_MISSING"
                            : "LIVE_EXECUTION_ACCOUNT_MISSING");
        }
        if (accountPurpose == ExecutionAccountPurpose.MICRO_LIVE && microLiveCapacityGate != null) {
            MicroLiveCapacityDecision capacity = microLiveCapacityGate.evaluate(
                    executionAccount, shadow.getIdUser(), validCapitalAsset(detail));
            if (!capacity.allowed()) {
                throw new ExecutionAccountUnavailableException(targetExecutionMode, capacity.reasonCode());
            }
        }

        CopySymbolResolution symbol = decision.symbolResolution();
        UserCopyAllocationEntity allocation = UserCopyAllocationEntity.builder()
                .idUser(shadow.getIdUser())
                .walletId(normalizeWallet(shadow.getWalletId()))
                .copyStrategyCode(normalizeStrategy(shadow.getCopyStrategyCode()))
                .copyStrategySlug(shadow.getCopyStrategySlug())
                .copyStrategyLabel(shadow.getCopyStrategyLabel())
                .copyMode(resolvedCopyMode)
                .strategySourceEndpoint(shadow.getStrategySourceEndpoint())
                .rankWithinStrategy(shadow.getRankWithinStrategy())
                .globalRank(shadow.getGlobalRank())
                .strategyScore(shadow.getStrategyScore())
                .allocationPct(allocationPct)
                .sizingMode("LIVE".equals(targetExecutionMode) ? "PERCENTAGE" : "FIXED_CAPITAL")
                .allocationPctSource("LIVE".equals(targetExecutionMode)
                        ? percentageResolution.source()
                        : "FIXED_MICRO_BUDGET")
                .allocationPctSourceId("LIVE".equals(targetExecutionMode)
                        ? percentageResolution.sourceId()
                        : null)
                .allocationPctCalculatedAt("LIVE".equals(targetExecutionMode)
                        ? percentageResolution.calculatedAt()
                        : null)
                .allocationPctValidUntil("LIVE".equals(targetExecutionMode)
                        ? percentageResolution.validUntil()
                        : null)
                .walletTotalAllocationPct("LIVE".equals(targetExecutionMode)
                        ? percentageResolution.walletTotalPercentage()
                        : null)
                .score(shadow.getDecisionScore())
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .executionMode(targetExecutionMode)
                .exchangeAccountId(executionAccount.getId())
                .reservedCapitalUsd("MICRO_LIVE".equals(targetExecutionMode)
                        ? microLiveBudgetPerAllocation() : null)
                .statusReason("LIVE".equals(targetExecutionMode) ? "PROMOTED_DIRECT_FROM_SHADOW" : "PROMOTED_FROM_SHADOW")
                .statusUpdatedAt(now)
                .updatedAt(now)
                .scopeType(normalizeScopeType(shadow.getScopeType()))
                .scopeValue(normalizeScopeValue(shadow.getScopeValue(), shadow.getCopyStrategyCode()))
                .strategyKey(strategyKey(shadow))
                .walletProfileId(shadow.getWalletProfileId())
                .linkedShadowAllocationId(shadow.getId())
                .promotedFromShadowAt(now)
                .sourceRankingVersion(shadow.getSourceRankingVersion())
                .sourceSymbol(symbol == null ? null : symbol.sourceSymbol())
                .targetSymbol(symbol == null ? null : symbol.targetSymbol())
                .capitalAsset(symbol == null ? validCapitalAsset(detail) : symbol.capitalAsset())
                .resolvedQuoteAsset(symbol == null ? null : symbol.quoteAsset())
                .symbolResolutionStatus(symbol == null ? null : "RESOLVED")
                .symbolResolutionReason(symbol == null ? null : symbol.reasonCode())
                .copyMinNotionalMode(detail.getCopyMinNotionalMode())
                .copyMinNotionalMaxUsdt(detail.getCopyMinNotionalMaxUsdt())
                .copyMinNotionalMinScore(detail.getCopyMinNotionalMinScore())
                .copyMinNotionalMinHistoryDays(detail.getCopyMinNotionalMinHistoryDays())
                .copyMinNotionalMinOperations(detail.getCopyMinNotionalMinOperations())
                .build();
        allocation = allocationRepository.saveAndFlush(allocation);
        decision.details().put("LIVE".equals(targetExecutionMode) ? "liveAllocationReasonCode" : "microLiveAllocationReasonCode",
                "LIVE".equals(targetExecutionMode) ? "LIVE_ALLOCATION_CREATED" : "MICRO_LIVE_ALLOCATION_CREATED");

        recordShadowPostPromotion(shadow, allocation, targetExecutionMode, now, decision.details());
        if ("LIVE".equals(targetExecutionMode)) {
            recordLivePromotion("promoted", "LIVE_ALLOCATION_PCT_RESOLVED");
            log.info("event=copy.promotion.live.percentage.resolved executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} previousExecutionMode=SHADOW previousAllocationPct={} resolvedLiveAllocationPct={} walletTotalAllocationPct={} allocationPctSource={} distributionDecisionId={} distributionCalculatedAt={} usesAllocationPctForSizing=true decision=PROMOTE reasonCode=LIVE_ALLOCATION_PCT_RESOLVED",
                    shadow.getIdUser(), shadow.getWalletId(), shadow.getCopyStrategyCode(), shadow.getScopeType(),
                    shadow.getScopeValue(), shadow.getAllocationPct(), percentageResolution.percentage(),
                    percentageResolution.walletTotalPercentage(), percentageResolution.source(),
                    percentageResolution.sourceId(), percentageResolution.calculatedAt());
        } else {
            log.info("event=copy.allocation.percentage.resolved executionMode=MICRO_LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} allocationPct=null allocationPctSource=FIXED_MICRO_BUDGET allocatedCapitalUsd=100 usesAllocationPctForSizing=false decision=ALLOW reasonCode=MICRO_LIVE_FIXED_BUDGET_NO_PCT",
                    shadow.getIdUser(), shadow.getWalletId(), shadow.getCopyStrategyCode(), shadow.getScopeType(),
                    shadow.getScopeValue());
        }
        return new PromotionOutput(plan, allocation);
    }

    private void recordShadowPostPromotion(
            ShadowCopyAllocationEntity shadow,
            UserCopyAllocationEntity allocation,
            String targetExecutionMode,
            OffsetDateTime now,
            Map<String, Object> details
    ) {
        String oldStatus = shadow.getStatus();
        String oldReason = shadow.getLastValidationReason();
        String executionMode = normalizeTargetMode(targetExecutionMode);
        String newStatus = "LIVE".equals(executionMode) ? SHADOW_STATUS_PROMOTED_TO_LIVE : SHADOW_STATUS_VALIDATED;
        String newReason = "LIVE".equals(executionMode) ? "LIVE_READY_FROM_SHADOW" : PROMOTED_TO_MICRO_REASON;

        shadow.setLinkedLiveAllocationId(allocation.getId());
        shadow.setPromotedToLiveAt(now);
        shadow.setStatus(newStatus);
        shadow.setLastValidationReason(newReason);
        shadow.setUpdatedAt(now);
        if (details != null) {
            details.put("shadowPromotionStatus", newStatus);
            details.put("shadowPromotionReasonCode", newReason);
            details.put("shadowStatusConstraintReasonCode", "SHADOW_STATUS_CONSTRAINT_SAFE");
            details.put("shadowPromotionStatusRecorded", true);
        }
        shadowAllocationRepository.saveAndFlush(shadow);
        log.info(
                "event=copy.promotion.shadow_post_promote.status_safe userId={} walletId={} shadowAllocationId={} microLiveAllocationId={} strategyCode={} scopeType={} scopeValue={} copyGuardStatus={} copyGuardAction={} lastValidationReason={} executionMode={} oldStatus={} newStatus={} reasonCode={} decision=RECORDED",
                shadow.getIdUser(),
                shadow.getWalletId(),
                shadow.getId(),
                allocation.getId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                shadow.getCopyGuardStatus(),
                shadow.getCopyGuardAction(),
                oldReason,
                executionMode,
                oldStatus,
                newStatus,
                "SHADOW_STATUS_CONSTRAINT_SAFE"
        );
    }

    private PlanResolution resolveCopyPlan(
            ShadowCopyAllocationEntity shadow,
            DetailUserEntity detail,
            PromotionDecision decision,
            OffsetDateTime now,
            BigDecimal allocatedCapital,
        LiveAllocationPercentageResolution percentageResolution
    ) {
        String walletLc = normalizeWallet(shadow.getWalletId());
        boolean created = planRepository.ensureFixedBudgetPlan(shadow.getIdUser(), walletLc, now) > 0;
        UserWalletCopyPlanEntity plan = planRepository.findForUpdate(shadow.getIdUser(), walletLc)
                .orElseThrow(() -> new IllegalStateException("USER_WALLET_COPY_PLAN_MISSING_AFTER_UPSERT"));
        applyPlanFields(plan, shadow, detail, decision, now, allocatedCapital,
                percentageResolution);
        plan = planRepository.saveAndFlush(plan);
        String reasonCode = created ? "COPY_PLAN_CREATED" : "COPY_PLAN_ALREADY_EXISTS";
        logCopyPlanResolved(
                shadow,
                plan,
                reasonCode,
                normalizeTargetMode(decision.targetExecutionMode()),
                created ? "CREATED" : "REUSED");
        return new PlanResolution(plan, reasonCode);
    }

    private void applyPlanFields(
            UserWalletCopyPlanEntity plan,
            ShadowCopyAllocationEntity shadow,
            DetailUserEntity detail,
            PromotionDecision decision,
            OffsetDateTime now,
            BigDecimal allocatedCapital,
            LiveAllocationPercentageResolution percentageResolution
    ) {
        boolean liveTarget = "LIVE".equals(decision.targetExecutionMode());
        boolean preserveExistingLivePlan = !liveTarget
                && ("PERCENTAGE".equals(plan.getSizingMode())
                || (plan.getAllocationPct() != null
                && plan.getAllocationPct().signum() > 0
                && !"FIXED_MICRO_BUDGET".equals(plan.getAllocationPctSource())));
        if (liveTarget) {
            plan.setAllocationPct(percentageResolution.walletTotalPercentage());
            plan.setWalletTotalAllocationPct(percentageResolution.walletTotalPercentage());
            plan.setSizingMode("PERCENTAGE");
            plan.setAllocationPctSource(percentageResolution.source());
            plan.setAllocationPctSourceId(percentageResolution.sourceId());
            plan.setAllocationPctCalculatedAt(percentageResolution.calculatedAt());
            plan.setAllocationPctValidUntil(percentageResolution.validUntil());
            plan.setAllocatedCapitalUsd(allocatedCapital.setScale(8, RoundingMode.HALF_UP));
        } else if (!preserveExistingLivePlan) {
            plan.setAllocationPct(null);
            plan.setWalletTotalAllocationPct(null);
            plan.setSizingMode("FIXED_CAPITAL");
            plan.setAllocationPctSource("FIXED_MICRO_BUDGET");
            plan.setAllocationPctSourceId(null);
            plan.setAllocationPctCalculatedAt(null);
            plan.setAllocationPctValidUntil(null);
            plan.setAllocatedCapitalUsd(allocatedCapital.setScale(8, RoundingMode.HALF_UP));
        }
        plan.setScore(shadow.getDecisionScore());
        plan.setStatus("ACTIVE");
        plan.setActive(true);
        plan.setMetricVersion(1);
        plan.setMaxWallet(detail.getMaxWallet());
        plan.setUserCapitalUsd(BigDecimal.valueOf(Math.max(0, detail.getCapital() == null ? 0 : detail.getCapital())).setScale(8, RoundingMode.HALF_UP));
        plan.setCopyMinNotionalMode(detail.getCopyMinNotionalMode() == null ? "INHERIT" : detail.getCopyMinNotionalMode().name());
        plan.setCopyMinNotionalMaxUsdt(detail.getCopyMinNotionalMaxUsdt());
        plan.setCopyMinNotionalMinScore(detail.getCopyMinNotionalMinScore());
        plan.setCopyMinNotionalMinHistoryDays(detail.getCopyMinNotionalMinHistoryDays());
        plan.setCopyMinNotionalMinOperations(detail.getCopyMinNotionalMinOperations());
        plan.setReason(decision.details());
        plan.setSyncedToRuntime(true);
        plan.setRuntimeSyncedAt(now);
        plan.setUpdatedAt(now);
    }

    private void logCopyPlanResolved(
            ShadowCopyAllocationEntity shadow,
            UserWalletCopyPlanEntity plan,
            String reasonCode,
            String executionMode,
            String decision
    ) {
        String event = "COPY_PLAN_CREATED".equals(reasonCode)
                ? "copy.promotion.copy_plan.created"
                : "copy.promotion.copy_plan.reused";
        log.info(
                "event={} userId={} walletId={} shadowAllocationId={} planId={} strategyCode={} scopeType={} scopeValue={} reasonCode={} executionMode={} decision={}",
                event,
                shadow.getIdUser(),
                shadow.getWalletId(),
                shadow.getId(),
                plan == null ? null : plan.getId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                reasonCode,
                executionMode,
                decision
        );
    }

    private CopyModeResolution resolveUserCopyAllocationCopyMode(ShadowCopyAllocationEntity shadow) {
        return UserCopyAllocationCopyModeResolver.resolve(
                shadow == null ? null : shadow.getCopyStrategyCode(),
                shadow == null ? null : shadow.getCopyMode()
        );
    }

    private void logCopyModeResolved(ShadowCopyAllocationEntity shadow, CopyModeResolution resolution, String decision) {
        log.info(
                "event=copy.promotion.copy_mode.resolved userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} reasonCode={} constraintReasonCode={} executionMode=MICRO_LIVE decision={}",
                shadow == null ? null : shadow.getIdUser(),
                shadow == null ? null : shadow.getWalletId(),
                shadow == null ? null : shadow.getId(),
                shadow == null ? null : shadow.getCopyStrategyCode(),
                shadow == null ? null : shadow.getScopeType(),
                shadow == null ? null : shadow.getScopeValue(),
                shadow == null ? null : safe(shadow.getCopyMode()),
                resolution == null ? null : resolution.copyMode(),
                resolution == null ? null : resolution.reasonCode(),
                resolution == null ? null : resolution.constraintReasonCode(),
                decision
        );
    }

    private DirectLiveDecision directLiveDecision(ShadowCopyAllocationEntity shadow) {
        String requestedTarget = normalizeTargetMode(properties == null ? null : properties.getDefaultTargetMode());
        String policy = normalizePolicy(properties == null ? null : properties.getDirectLivePolicy());
        if (!"LIVE".equals(requestedTarget)) {
            return new DirectLiveDecision(true, "MICRO_LIVE", policy, "MICRO_LIVE_REQUIRED_BY_POLICY");
        }
        if (!"ALLOW_DIRECT_LIVE_FOR_LIVE_READY".equals(policy)) {
            return new DirectLiveDecision(false, "LIVE", policy, "MICRO_LIVE_REQUIRED_BY_POLICY");
        }
        if (!shadowReadyForDirectLive(shadow)) {
            return new DirectLiveDecision(false, "LIVE", policy, "LIVE_NOT_READY_FROM_SHADOW");
        }
        return new DirectLiveDecision(true, "LIVE", policy, "DIRECT_LIVE_ALLOWED_BY_POLICY");
    }

    private LiveAllocationPercentageResolution resolveLivePercentage(
            ShadowCopyAllocationEntity shadow,
            OffsetDateTime promotionTime
    ) {
        if (liveAllocationPercentageResolver == null) {
            return LiveAllocationPercentageResolution.rejected("LIVE_DISTRIBUTION_NOT_AVAILABLE");
        }
        try {
            return liveAllocationPercentageResolver.resolve(new LiveAllocationPercentageRequest(
                    shadow.getIdUser(),
                    shadow.getWalletId(),
                    shadow.getCopyStrategyCode(),
                    normalizeScopeType(shadow.getScopeType()),
                    normalizeScopeValue(shadow.getScopeValue(), shadow.getCopyStrategyCode()),
                    promotionTime));
        } catch (RuntimeException ex) {
            log.warn("event=copy.promotion.live.rejected executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} previousExecutionMode=SHADOW previousAllocationPct={} resolvedLiveAllocationPct=null allocationPctSource=null decision=KEEP_SHADOW reasonCode=LIVE_DISTRIBUTION_NOT_AVAILABLE retryable=true shouldAlert=false recommendedAction=WAIT_FOR_NEXT_VALID_DISTRIBUTION errorClass={} errorMessage=\"{}\"",
                    shadow.getIdUser(), shadow.getWalletId(), shadow.getCopyStrategyCode(), shadow.getScopeType(),
                    shadow.getScopeValue(), shadow.getAllocationPct(), ex.getClass().getSimpleName(), safe(ex.getMessage()));
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

    private void logDirectLivePolicyChecked(ShadowCopyAllocationEntity shadow, DirectLiveDecision decision) {
        log.info(
                "event=copy.promotion.direct_live.policy_checked userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode={} decision={} reasonCode={} policy={}",
                shadow == null ? null : shadow.getIdUser(),
                shadow == null ? null : shadow.getWalletId(),
                shadow == null ? null : shadow.getId(),
                shadow == null ? null : shadow.getCopyStrategyCode(),
                shadow == null ? null : shadow.getScopeType(),
                shadow == null ? null : shadow.getScopeValue(),
                shadow == null ? null : safe(shadow.getCopyMode()),
                null,
                decision == null ? null : decision.targetExecutionMode(),
                decision != null && decision.allowed() ? "ALLOW" : "REJECT",
                decision == null ? null : decision.reasonCode(),
                decision == null ? null : decision.policy()
        );
    }

    private boolean shadowReadyForDirectLive(ShadowCopyAllocationEntity shadow) {
        String reason = normalizeToken(shadow == null ? null : shadow.getLastValidationReason());
        return "LIVE_READY_FROM_SHADOW".equals(reason) || "SHADOW_FILTERS_PASSED".equals(reason);
    }

    private Evidence evidence(
            ShadowCopyAllocationEntity shadow,
            ShadowWalletProfileValidationEntity validation,
            OffsetDateTime now,
            ShadowCoverageBatch coverageBatch
    ) {
        long recordedSnapshot = validation == null || validation.getRecordedEvents() == null
                ? 0L
                : Math.max(0L, validation.getRecordedEvents());
        long simulatedSnapshot = validation == null || validation.getSimulatedEvents() == null
                ? 0L
                : Math.max(0L, validation.getSimulatedEvents());
        long events = Math.max(recordedSnapshot, simulatedSnapshot);
        if (events <= 0L) {
            events = shadowEventRepository.countByShadowAllocationId(shadow.getId());
        }
        long skipped = validation == null || validation.getSkippedEvents() == null ? 0L : Math.max(0L, validation.getSkippedEvents());
        long errors = validation == null || validation.getErrorEvents() == null ? 0L : Math.max(0L, validation.getErrorEvents());
        long closed = validation == null || validation.getClosedPositions() == null
                ? shadowPositionStateRepository.countClosedPositions(shadow.getId())
                : Math.max(0L, validation.getClosedPositions());
        BigDecimal pnl = validation == null || validation.getNetPnlUsd() == null
                ? nullToZero(shadowPositionStateRepository.sumClosedRealizedPnlUsd(shadow.getId()))
                : validation.getNetPnlUsd();
        BigDecimal drawdown = validation == null || validation.getMaxDrawdown() == null
                ? ZERO
                : validation.getMaxDrawdown();
        OffsetDateTime createdAt = firstNonNull(shadow.getCreatedAt(), shadow.getLastSeenAt(), now);
        long days = Math.max(0L, Duration.between(createdAt, now).toDays());
        BigDecimal coverage = coveragePct(events, skipped, errors);
        ShadowCoverageBatch batch = coverageBatch == null
                ? ShadowCoverageBatch.failure(
                List.of(shadow.getId()),
                now.minusDays(shadowCoverageProperties.getWindowDays()),
                now,
                new IllegalStateException("missing shadow coverage batch")
        )
                : coverageBatch;
        OffsetDateTime coverageWindowEnd = batch.windowEnd() == null ? now : batch.windowEnd();
        ShadowCoverageSnapshot coverageSnapshot = shadowCoverageCalculator.calculate(
                events,
                skipped,
                errors,
                batch.countsFor(shadow.getId()),
                batch.queryFailed(),
                coverageWindowEnd,
                shadowCoverageProperties,
                properties.getMinShadowCoveragePct()
        );
        return new Evidence(days, events, skipped, errors, closed, pnl, drawdown, coverage, coverageSnapshot);
    }

    private CopySymbolResolution resolveSymbol(ShadowCopyAllocationEntity shadow, String capitalAsset) {
        String sourceSymbol = sourceSymbol(shadow);
        if (sourceSymbol == null) {
            return null;
        }
        return copySymbolResolver.resolve(sourceSymbol, capitalAsset);
    }

    private void markRejected(ShadowCopyAllocationEntity shadow, PromotionDecision decision, OffsetDateTime now) {
        shadow.setStatus("SHADOW_ACTIVE");
        shadow.setLastValidationReason(decision.reasonCode());
        shadow.setUpdatedAt(now);
        shadowAllocationRepository.save(shadow);
    }

    private Optional<UserCopyAllocationEntity> existingAllocation(ShadowCopyAllocationEntity shadow) {
        return allocationRepository.findOpenAllocationForUserWalletStrategyScopeAndMode(
                shadow.getIdUser(),
                shadow.getWalletId(),
                normalizeStrategy(shadow.getCopyStrategyCode()),
                normalizeScopeType(shadow.getScopeType()),
                normalizeScopeValue(shadow.getScopeValue(), shadow.getCopyStrategyCode()),
                "MICRO_LIVE"
        );
    }

    private void linkShadowToExisting(ShadowCopyAllocationEntity shadow, UserCopyAllocationEntity allocation, OffsetDateTime now) {
        if (shadow == null || allocation == null) {
            return;
        }
        String oldStatus = shadow.getStatus();
        shadow.setLinkedLiveAllocationId(allocation.getId());
        shadow.setPromotedToLiveAt(firstNonNull(shadow.getPromotedToLiveAt(), allocation.getPromotedFromShadowAt(), now));
        String allocationMode = normalizeTargetMode(allocation.getExecutionMode());
        shadow.setStatus("LIVE".equals(allocationMode) ? SHADOW_STATUS_PROMOTED_TO_LIVE : SHADOW_STATUS_VALIDATED);
        shadow.setLastValidationReason("ALREADY_PROMOTED");
        shadow.setUpdatedAt(now);
        shadowAllocationRepository.save(shadow);
        log.info(
                "event=copy.promotion.shadow_post_promote.status_safe userId={} walletId={} shadowAllocationId={} microLiveAllocationId={} strategyCode={} scopeType={} scopeValue={} copyGuardStatus={} copyGuardAction={} lastValidationReason={} executionMode={} oldStatus={} newStatus={} reasonCode={} decision=NOOP",
                shadow.getIdUser(),
                shadow.getWalletId(),
                shadow.getId(),
                allocation.getId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                shadow.getCopyGuardStatus(),
                shadow.getCopyGuardAction(),
                shadow.getLastValidationReason(),
                allocationMode,
                oldStatus,
                shadow.getStatus(),
                "SHADOW_STATUS_CONSTRAINT_SAFE"
        );
    }

    private void audit(
            ShadowCopyAllocationEntity shadow,
            PromotionDecision decision,
            UserCopyAllocationEntity allocation,
            String event
    ) {
        auditRepository.save(CopyPromotionAuditEntity.builder()
                .idUser(shadow == null ? null : shadow.getIdUser())
                .walletId(shadow == null ? null : shadow.getWalletId())
                .copyStrategyCode(shadow == null ? null : shadow.getCopyStrategyCode())
                .sourceExecutionMode("SHADOW")
                .targetExecutionMode(decision.allowed() ? decision.targetExecutionMode() : "SHADOW")
                .decision(event)
                .reasonCode(decision.reasonCode())
                .reasonDetails(decision.details())
                .shadowAllocationId(shadow == null ? null : shadow.getId())
                .microLiveAllocationId(allocation == null ? null : allocation.getId())
                .build());
    }

    private PromotionDecision rejected(String reason, ShadowCopyAllocationEntity shadow) {
        return PromotionDecision.rejected(reason, details(shadow, null, Map.of()));
    }

    private Map<String, Object> details(ShadowCopyAllocationEntity shadow, Evidence evidence, Map<String, Object> extra) {
        Map<String, Object> details = new LinkedHashMap<>();
        if (shadow != null) {
            details.put("shadowAllocationId", shadow.getId());
            details.put("walletId", shadow.getWalletId());
            details.put("strategyCode", shadow.getCopyStrategyCode());
            details.put("scopeType", shadow.getScopeType());
            details.put("scopeValue", shadow.getScopeValue());
            details.put("copyGuardStatus", shadow.getCopyGuardStatus());
            details.put("copyGuardAction", shadow.getCopyGuardAction());
            details.put("lastValidationReason", shadow.getLastValidationReason());
        }
        if (evidence != null) {
            details.put("shadowDays", evidence.shadowDays());
            details.put("events", evidence.events());
            details.put("closedPositions", evidence.closedPositions());
            details.put("coveragePct", evidence.coveragePct());
            details.put("netPnlUsd", evidence.netPnlUsd());
            details.put("maxDrawdownPct", evidence.maxDrawdownPct());
            ShadowCoverageSnapshot coverage = evidence.coverageSnapshot();
            details.put("historicalSimulatedEvents", coverage.historicalSimulatedEvents());
            details.put("historicalSkippedEvents", coverage.historicalSkippedEvents());
            details.put("historicalErrorEvents", coverage.historicalErrorEvents());
            details.put("historicalCoveragePct", coverage.historicalCoveragePct());
            details.put("coverageHistoricalPct", coverage.historicalCoveragePct());
            details.put("rollingSimulatedEvents", coverage.rollingSimulatedEvents());
            details.put("rollingRecordedEvents", coverage.rollingRecordedEvents());
            details.put("rollingSkippedEvents", coverage.rollingSkippedEvents());
            details.put("rollingErrorEvents", coverage.rollingErrorEvents());
            details.put("rollingEvaluableEvents", coverage.rollingEvaluableEvents());
            details.put("rollingCoveragePct", coverage.rollingCoveragePct());
            details.put("coverageRollingPct", coverage.rollingCoveragePct());
            details.put("rollingWindowDays", coverage.rollingWindowDays());
            details.put("rollingMaxEvents", coverage.rollingMaxEvents());
            details.put("rollingMinEvents", coverage.rollingMinEvents());
            details.put("rollingWindowStart", coverage.rollingWindowStart().toString());
            details.put("rollingWindowEnd", coverage.rollingWindowEnd().toString());
            details.put("coverageThresholdPct", coverage.coverageThresholdPct());
            details.put("rollingThresholdPct", coverage.rollingThresholdPct());
            details.put("coverageSourceUsed", coverage.coverageSourceUsed().name());
            details.put("coverageDecision", coverage.coverageDecision().name());
            details.put("coverageReasonCode", coverage.coverageReasonCode().name());
            details.put("rollingCoverageDecision", coverage.rollingCoverageDecision().name());
            details.put("rollingCoverageReasonCode", coverage.rollingCoverageReasonCode().name());
        }
        if (extra != null) {
            extra.forEach((key, value) -> {
                if (value != null) details.put(key, value);
            });
        }
        return details;
    }

    private void logCoverageEvaluated(ShadowCopyAllocationEntity shadow, ShadowCoverageSnapshot coverage) {
        log.info(
                "event=shadow.coverage.evaluated shadowAllocationId={} walletId={} strategyCode={} scopeType={} scopeValue={} historicalSimulated={} historicalSkipped={} historicalErrors={} historicalCoveragePct={} rollingSimulated={} rollingSkipped={} rollingErrors={} rollingEvaluable={} rollingCoveragePct={} windowDays={} maxEvents={} minEvents={} thresholdPct={} windowStart={} windowEnd={} coverageSourceUsed={} decision={} reasonCode={}",
                shadow.getId(),
                shadow.getWalletId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                coverage.historicalSimulatedEvents(),
                coverage.historicalSkippedEvents(),
                coverage.historicalErrorEvents(),
                coverage.historicalCoveragePct(),
                coverage.rollingSimulatedEvents(),
                coverage.rollingSkippedEvents(),
                coverage.rollingErrorEvents(),
                coverage.rollingEvaluableEvents(),
                coverage.rollingCoveragePct(),
                coverage.rollingWindowDays(),
                coverage.rollingMaxEvents(),
                coverage.rollingMinEvents(),
                coverage.coverageThresholdPct(),
                coverage.rollingWindowStart(),
                coverage.rollingWindowEnd(),
                coverage.coverageSourceUsed(),
                coverage.coverageDecision(),
                coverage.coverageReasonCode()
        );
    }

    private void logCoverageBlocked(ShadowCopyAllocationEntity shadow, ShadowCoverageSnapshot coverage) {
        log.info(
                "event=shadow.promotion.coverage_blocked shadowAllocationId={} walletId={} strategyCode={} scopeType={} scopeValue={} historicalCoveragePct={} rollingCoveragePct={} rollingEvaluable={} requiredCoveragePct={} requiredMinEvents={} coverageSourceUsed={} decision={} reasonCode={}",
                shadow.getId(),
                shadow.getWalletId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                coverage.historicalCoveragePct(),
                coverage.rollingCoveragePct(),
                coverage.rollingEvaluableEvents(),
                coverage.coverageThresholdPct(),
                coverage.rollingMinEvents(),
                coverage.coverageSourceUsed(),
                coverage.coverageDecision(),
                coverage.coverageReasonCode()
        );
    }

    private static Map<String, Object> extras(Object... keyValues) {
        Map<String, Object> values = new LinkedHashMap<>();
        if (keyValues == null) {
            return values;
        }
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            Object key = keyValues[i];
            if (key != null) {
                values.put(String.valueOf(key), keyValues[i + 1]);
            }
        }
        return values;
    }

    private boolean copyGuardOpen(ShadowCopyAllocationEntity shadow) {
        String action = normalizeToken(shadow.getCopyGuardAction());
        String status = normalizeToken(shadow.getCopyGuardStatus());
        if ("SHADOW_ONLY".equals(action) && properties.isAllowShadowOnlySummary()) {
            return true;
        }
        return !("PAUSE_OPEN".equals(action) || "SHADOW_ONLY".equals(action) || "DISABLED".equals(action)
                || "BLOCKED".equals(action) || "PAUSE_OPEN".equals(status) || "SHADOW_ONLY".equals(status)
                || "DISABLED".equals(status) || "DATA_RISK".equals(status) || "BLOCKED".equals(status));
    }

    private boolean summaryOnlyBlockedBySummary(ShadowCopyAllocationEntity shadow) {
        String reason = normalizeToken(shadow == null ? null : shadow.getLastValidationReason());
        if (!"SUMMARY_NOT_FINAL_LIVE_BLOCKED".equals(reason)
                && !"SUMMARY_OR_MISSING_FACT_PAYLOAD_REQUIRES_SHADOW_OR_FULL_VALIDATION".equals(reason)) {
            return false;
        }
        return summaryOnlyToken(normalizeToken(shadow.getCopyGuardAction()))
                && summaryOnlyToken(normalizeToken(shadow.getCopyGuardStatus()));
    }

    private static boolean summaryOnlyToken(String value) {
        return value == null || value.isBlank()
                || "OK".equals(value)
                || "ALLOW".equals(value)
                || "SHADOW_ONLY".equals(value);
    }

    private boolean inactive(ShadowCopyAllocationEntity shadow, OffsetDateTime now) {
        OffsetDateTime last = firstNonNull(
                shadow.getStrategyLastActivityAt(),
                shadow.getWalletLastActivityAt(),
                shadow.getLastSeenAt(),
                shadow.getUpdatedAt(),
                shadow.getCreatedAt()
        );
        if (last == null) {
            return true;
        }
        return Duration.between(last, now).toDays() > Math.max(0L, properties.getMaxInactiveDays());
    }

    private static boolean usable(UserApiKeyEntity apiKey) {
        return apiKey != null
                && apiKey.getApiKey() != null && !apiKey.getApiKey().isBlank()
                && apiKey.getApiSecret() != null && !apiKey.getApiSecret().isBlank();
    }

    private BigDecimal microLiveCapital(BigDecimal userCapital) {
        BigDecimal capital = nullToZero(userCapital);
        BigDecimal initial = positiveOrDefault(properties.getMicroLiveInitialCapitalUsd(), new BigDecimal("100"));
        BigDecimal max = positiveOrDefault(properties.getMicroLiveMaxCapitalUsd(), initial);
        return capital.min(initial).min(max).max(ZERO).setScale(8, RoundingMode.HALF_UP);
    }

    private static BigDecimal coveragePct(long recorded, long skipped, long errors) {
        long total = Math.max(0L, recorded) + Math.max(0L, skipped) + Math.max(0L, errors);
        if (total == 0L) return ZERO;
        return BigDecimal.valueOf(recorded)
                .multiply(HUNDRED)
                .divide(BigDecimal.valueOf(total), 6, RoundingMode.HALF_UP);
    }

    private static String sourceSymbol(ShadowCopyAllocationEntity shadow) {
        if (!"SYMBOL_SPECIALIST".equals(normalizeStrategy(shadow.getCopyStrategyCode()))) {
            return null;
        }
        String value = shadow.getScopeValue();
        if (value == null || value.isBlank()) return null;
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        if ("ALL".equals(normalized) || "DEFAULT".equals(normalized) || "SYMBOL_SPECIALIST".equals(normalized)) {
            return null;
        }
        return normalized;
    }

    private static String strategyKey(ShadowCopyAllocationEntity shadow) {
        return MetricStrategyIdentity.canonicalKey(
                shadow.getWalletId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue()
        );
    }

    private static BigDecimal positiveOrDefault(BigDecimal value, BigDecimal fallback) {
        return value == null || value.compareTo(ZERO) <= 0 ? fallback : value;
    }

    private static BigDecimal nullToZero(BigDecimal value) {
        return value == null ? ZERO : value;
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values) {
        if (values == null) return null;
        for (T value : values) {
            if (value != null) return value;
        }
        return null;
    }

    private CapitalConfig resolveCapitalConfig(DetailUserEntity detail) {
        if (detail == null) {
            return CapitalConfig.invalid("NO_ACTIVE_USER_DETAIL");
        }
        Integer capital = detail.getCapital();
        if (capital == null || capital <= 0) {
            return CapitalConfig.invalid("CAPITAL_MISSING_OR_NON_POSITIVE");
        }
        String capitalAsset = detail.getCapitalAsset();
        if (!FuturesCapitalAsset.isAllowedStable(capitalAsset)) {
            return CapitalConfig.invalid("CAPITAL_ASSET_MISSING_OR_INVALID");
        }
        Integer maxWallet = detail.getMaxWallet();
        if (maxWallet == null || maxWallet <= 0) {
            return CapitalConfig.invalid("MAX_WALLET_MISSING_OR_NON_POSITIVE");
        }
        return new CapitalConfig(true, "CAPITAL_CONFIG_FOUND_FROM_USER_DETAIL", capital, maxWallet, capitalAsset.trim().toUpperCase(Locale.ROOT));
    }

    private static String validCapitalAsset(DetailUserEntity detail) {
        return FuturesCapitalAsset.fromNullable(detail == null ? null : detail.getCapitalAsset()).name();
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

    private UserApiKeyEntity executionAccount(UUID userId, ExecutionAccountPurpose purpose) {
        return userApiKeyRepository
                .findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(
                        userId, "BINANCE", purpose)
                .orElse(null);
    }

    private static boolean containsReason(Throwable error, String reason) {
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(reason)) return true;
            current = current.getCause();
        }
        return false;
    }

    private static final class ExecutionAccountUnavailableException extends RuntimeException {
        private final String executionMode;
        private final String reasonCode;

        private ExecutionAccountUnavailableException(String executionMode, String reasonCode) {
            super(reasonCode);
            this.executionMode = executionMode;
            this.reasonCode = reasonCode;
        }

        private String executionMode() { return executionMode; }
        private String reasonCode() { return reasonCode; }
    }

    private static String normalizeToken(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeTargetMode(String value) {
        String normalized = normalizeToken(value);
        return "LIVE".equals(normalized) ? "LIVE" : "MICRO_LIVE";
    }

    private static boolean isDirectLiveReason(String reasonCode) {
        String normalized = normalizeToken(reasonCode);
        return "DIRECT_LIVE_DISABLED_BY_POLICY".equals(normalized)
                || "DIRECT_LIVE_ALLOWED_BY_POLICY".equals(normalized)
                || "MICRO_LIVE_REQUIRED_BY_POLICY".equals(normalized)
                || "LIVE_READY_FROM_SHADOW".equals(normalized)
                || "LIVE_NOT_READY_FROM_SHADOW".equals(normalized)
                || isLivePercentageReason(normalized);
    }

    private static boolean isLivePercentageReason(String reasonCode) {
        String normalized = normalizeToken(reasonCode);
        return normalized.startsWith("LIVE_ALLOCATION_PCT_")
                || normalized.startsWith("LIVE_DISTRIBUTION_");
    }

    private static String normalizePolicy(String value) {
        String normalized = normalizeToken(value);
        return "ALLOW_DIRECT_LIVE_FOR_LIVE_READY".equals(normalized)
                ? "ALLOW_DIRECT_LIVE_FOR_LIVE_READY"
                : "REQUIRE_MICRO_LIVE";
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    private static String safe(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim();
        return clean.length() > 300 ? clean.substring(0, 300) : clean;
    }

    private void recordLivePromotion(String result, String reason) {
        if (meterRegistry == null) return;
        meterRegistry.counter("copy_live_promotion_total",
                "result", safeMetricTag(result), "reason", safeMetricTag(reason)).increment();
    }

    private static String safeMetricTag(String value) {
        if (value == null || value.isBlank()) return "unknown";
        String normalized = value.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
        return normalized.length() > 80 ? normalized.substring(0, 80) : normalized;
    }

    private static int clampInt(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
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
        return extras(
                "fullDecisionRequired", true,
                "fullDecisionReasonCode", gate.reasonCode(),
                "fullDecisionResponseReasonCode", decision == null ? null : decision.getReasonCode(),
                "fullDecisionResponseReasonDetail", decision == null ? null : decision.getReasonDetail(),
                "fullDecisionMode", decision == null ? null : decision.getMode(),
                "fullDecisionSimulationMode", decision == null ? null : decision.getSimulationMode(),
                "fullDecisionMaterialized", decision != null && decision.isFullMaterialized(),
                "fullDecisionFactPayloadLoaded", decision != null && decision.isFactPayloadLoaded(),
                "fullDecisionFinal", decision != null && decision.isDecisionFinal(),
                "fullDecisionRequiresFullSimulation", decision != null && decision.isRequiresFullSimulation(),
                "fullDecisionCanMicroLive", decision != null && decision.isCanMicroLive(),
                "fullDecisionCanLive", decision != null && decision.isCanLive(),
                "fullDecisionAllowNewEntries", decision != null && decision.isAllowNewEntries(),
                "fullDecisionCopyGuardStatus", copyGuardStatus(decision),
                "fullDecisionCopyGuardAction", copyGuardAction(decision),
                "fullDecisionCopyGuardReasons", copyGuardReasons(decision),
                "fullDecisionElapsedMs", decision == null ? null : decision.getElapsedMs(),
                "fullDecisionFactsLoaded", decision == null ? null : decision.getFactsLoaded()
        );
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

    private static long elapsedMs(long startedNs) {
        return Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNs)).toMillis();
    }

    private record FullDecisionGate(
            boolean allowed,
            String reasonCode
    ) {
    }

    private record PromotionOutput(
            UserWalletCopyPlanEntity plan,
            UserCopyAllocationEntity allocation
    ) {
    }

    private record PromotionUnitOutcome(
            PromotionOutput output,
            boolean created
    ) {
    }

    private record PlanResolution(
            UserWalletCopyPlanEntity plan,
            String reasonCode
    ) {
    }

    private static final class LiveAllocationPercentageRejectedException extends RuntimeException {
        private final String reasonCode;

        private LiveAllocationPercentageRejectedException(String reasonCode) {
            super(reasonCode);
            this.reasonCode = reasonCode == null || reasonCode.isBlank()
                    ? "LIVE_DISTRIBUTION_NOT_AVAILABLE"
                    : reasonCode.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        }

        private String reasonCode() {
            return reasonCode;
        }
    }

    private record CapitalConfig(
            boolean valid,
            String reasonCode,
            Integer capital,
            Integer maxWallet,
            String capitalAsset
    ) {
        private static CapitalConfig invalid(String reasonCode) {
            return new CapitalConfig(false, reasonCode, null, null, null);
        }
    }

    private record DirectLiveDecision(
            boolean allowed,
            String targetExecutionMode,
            String policy,
            String reasonCode
    ) {
    }

    private BigDecimal microLiveBudgetPerAllocation() {
        BigDecimal configured = microLiveCapacityProperties == null
                ? null : microLiveCapacityProperties.getBudgetPerAllocationUsdc();
        return configured == null || configured.signum() <= 0 ? new BigDecimal("100") : configured;
    }

    private record Evidence(
            long shadowDays,
            long events,
            long skipped,
            long errors,
            long closedPositions,
            BigDecimal netPnlUsd,
            BigDecimal maxDrawdownPct,
            BigDecimal coveragePct,
            ShadowCoverageSnapshot coverageSnapshot
    ) {
        private Evidence {
            netPnlUsd = nullToZero(netPnlUsd);
            maxDrawdownPct = nullToZero(maxDrawdownPct);
            coveragePct = nullToZero(coveragePct);
            Objects.requireNonNull(coverageSnapshot, "coverageSnapshot");
        }
    }

    private record PromotionDecision(
            boolean allowed,
            String reasonCode,
            Map<String, Object> details,
            DetailUserEntity detail,
            Evidence evidence,
            CopySymbolResolution symbolResolution,
            BigDecimal microLiveCapital,
            String resolvedCopyMode,
            String targetExecutionMode,
            BigDecimal targetCapital,
            LiveAllocationPercentageResolution percentageResolution
    ) {
        private static PromotionDecision allowed(
                Map<String, Object> details,
                DetailUserEntity detail,
                Evidence evidence,
                CopySymbolResolution symbolResolution,
                BigDecimal microLiveCapital
        ) {
            return allowed(details, detail, evidence, symbolResolution, microLiveCapital, null);
        }

        private static PromotionDecision allowed(
                Map<String, Object> details,
                DetailUserEntity detail,
                Evidence evidence,
                CopySymbolResolution symbolResolution,
                BigDecimal microLiveCapital,
                String resolvedCopyMode
        ) {
            return allowed(details, detail, evidence, symbolResolution, microLiveCapital, resolvedCopyMode, "MICRO_LIVE", microLiveCapital);
        }

        private static PromotionDecision allowed(
                Map<String, Object> details,
                DetailUserEntity detail,
                Evidence evidence,
                CopySymbolResolution symbolResolution,
                BigDecimal microLiveCapital,
                String resolvedCopyMode,
                String targetExecutionMode,
                BigDecimal targetCapital
        ) {
            return allowed(details, detail, evidence, symbolResolution, microLiveCapital, resolvedCopyMode,
                    targetExecutionMode, targetCapital, null);
        }

        private static PromotionDecision allowed(
                Map<String, Object> details,
                DetailUserEntity detail,
                Evidence evidence,
                CopySymbolResolution symbolResolution,
                BigDecimal microLiveCapital,
                String resolvedCopyMode,
                String targetExecutionMode,
                BigDecimal targetCapital,
                LiveAllocationPercentageResolution percentageResolution
        ) {
            return new PromotionDecision(
                    true,
                    "SHADOW_VALIDATED_READY_FOR_MICRO",
                    details,
                    detail,
                    evidence,
                    symbolResolution,
                    nullToZero(microLiveCapital),
                    resolvedCopyMode,
                    targetExecutionMode == null || targetExecutionMode.isBlank() ? "MICRO_LIVE" : targetExecutionMode,
                    nullToZero(targetCapital),
                    percentageResolution
            );
        }

        private static PromotionDecision rejected(String reasonCode, Map<String, Object> details) {
            return new PromotionDecision(false, reasonCode, details, null, null, null, ZERO, null,
                    "SHADOW", ZERO, null);
        }
    }
}
