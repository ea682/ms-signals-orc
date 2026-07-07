package com.apunto.engine.service.impl;

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
import com.apunto.engine.service.ShadowPromotionService;
import com.apunto.engine.service.copy.promotion.ShadowPromotionProperties;
import com.apunto.engine.service.copy.promotion.ShadowPromotionResult;
import com.apunto.engine.service.copy.symbol.CopySymbolResolution;
import com.apunto.engine.service.copy.symbol.CopySymbolResolver;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class ShadowPromotionServiceImpl implements ShadowPromotionService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal HUNDRED = new BigDecimal("100");

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
        OffsetDateTime now = OffsetDateTime.now();

        for (ShadowCopyAllocationEntity shadow : candidates) {
            long candidateNs = System.nanoTime();
            if (shadow == null || shadow.getId() == null) {
                skipped++;
                continue;
            }
            evaluated++;
            try {
                PromotionDecision decision = evaluate(shadow, now);
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
                    existing.ifPresent(allocation -> linkShadowToExisting(shadow, allocation, now));
                    audit(shadow, decision, existing.orElse(null), "SHADOW_PROMOTION_NOOP");
                    log.info(
                            "event=copy.promotion.micro_live.noop userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} executionMode=MICRO_LIVE decision=NOOP reasonCode={} elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            decision.reasonCode(),
                            elapsedMs(candidateNs)
                    );
                    log.info(
                            "event=copy.promotion.shadow_to_micro.noop userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} executionMode=MICRO_LIVE decision=NOOP reasonCode={} elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            decision.reasonCode(),
                            elapsedMs(candidateNs)
                    );
                    continue;
                }
                if (!decision.allowed()) {
                    rejected++;
                    markRejected(shadow, decision, now);
                    audit(shadow, decision, null, "SHADOW_PROMOTION_REJECTED");
                    log.info(
                            "event=copy.promotion.shadow_to_micro.rejected userId={} walletId={} shadowAllocationId={} strategyCode={} scopeType={} scopeValue={} executionMode=SHADOW decision=REJECT reasonCode={} elapsedMs={} details={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            decision.reasonCode(),
                            elapsedMs(candidateNs),
                            decision.details()
                    );
                    continue;
                }
                ready++;
                PromotionOutput output = promote(shadow, decision, now);
                created++;
                audit(shadow, decision, output.allocation(), "MICRO_LIVE_CREATED");
                log.info(
                        "event=copy.promotion.micro_live.created userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} planId={} strategyCode={} scopeType={} scopeValue={} sourceSymbol={} targetSymbol={} executionMode=MICRO_LIVE decision=CREATED reasonCode={} capital={} elapsedMs={}",
                        shadow.getIdUser(),
                        shadow.getWalletId(),
                        shadow.getId(),
                        output.allocation().getId(),
                        output.plan().getId(),
                        shadow.getCopyStrategyCode(),
                        shadow.getScopeType(),
                        shadow.getScopeValue(),
                        decision.symbolResolution() == null ? sourceSymbol(shadow) : decision.symbolResolution().sourceSymbol(),
                        decision.symbolResolution() == null ? null : decision.symbolResolution().targetSymbol(),
                        decision.reasonCode(),
                        decision.microLiveCapital(),
                        elapsedMs(candidateNs)
                );
                log.info(
                        "event=copy.promotion.shadow_to_micro.created userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} planId={} strategyCode={} scopeType={} scopeValue={} sourceSymbol={} targetSymbol={} executionMode=MICRO_LIVE decision=CREATED reasonCode={} capital={} elapsedMs={}",
                        shadow.getIdUser(),
                        shadow.getWalletId(),
                        shadow.getId(),
                        output.allocation().getId(),
                        output.plan().getId(),
                        shadow.getCopyStrategyCode(),
                        shadow.getScopeType(),
                        shadow.getScopeValue(),
                        decision.symbolResolution() == null ? sourceSymbol(shadow) : decision.symbolResolution().sourceSymbol(),
                        decision.symbolResolution() == null ? null : decision.symbolResolution().targetSymbol(),
                        decision.reasonCode(),
                        decision.microLiveCapital(),
                        elapsedMs(candidateNs)
                );
            } catch (DataIntegrityViolationException ex) {
                Optional<UserCopyAllocationEntity> existing = existingAllocation(shadow);
                if (existing.isPresent()) {
                    skipped++;
                    PromotionDecision noop = PromotionDecision.rejected("ALREADY_PROMOTED", details(shadow, null, extras(
                            "existingAllocationId", existing.get().getId(),
                            "errorClass", ex.getClass().getSimpleName()
                    )));
                    linkShadowToExisting(shadow, existing.get(), now);
                    audit(shadow, noop, existing.get(), "SHADOW_PROMOTION_NOOP");
                    log.info(
                            "event=copy.promotion.micro_live.noop userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} strategyCode={} scopeType={} scopeValue={} executionMode=MICRO_LIVE decision=NOOP reasonCode=ALREADY_PROMOTED elapsedMs={}",
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
                            "event=copy.promotion.shadow_to_micro.noop userId={} walletId={} shadowAllocationId={} userCopyAllocationId={} strategyCode={} scopeType={} scopeValue={} executionMode=MICRO_LIVE decision=NOOP reasonCode=ALREADY_PROMOTED elapsedMs={}",
                            shadow.getIdUser(),
                            shadow.getWalletId(),
                            shadow.getId(),
                            existing.get().getId(),
                            shadow.getCopyStrategyCode(),
                            shadow.getScopeType(),
                            shadow.getScopeValue(),
                            elapsedMs(candidateNs)
                    );
                    continue;
                }
                rejected++;
                PromotionDecision failed = PromotionDecision.rejected("PROMOTION_FAILED_DUPLICATE_CONSTRAINT", extras(
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

    private PromotionDecision evaluate(ShadowCopyAllocationEntity shadow, OffsetDateTime now) {
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
        if (!detail.isApiKeyBinar() || !usable(userApiKeyRepository.findByUser_Id(shadow.getIdUser()))) {
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
        Evidence evidence = evidence(shadow, validation, now);
        PromotionDecision readiness = evaluateEvidence(shadow, evidence, now);
        if (!readiness.allowed()) {
            return readiness;
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

        BigDecimal microCapital = microLiveCapital(BigDecimal.valueOf(capitalConfig.capital()));
        return PromotionDecision.allowed(details(shadow, evidence, extras(
                "capital", capitalConfig.capital(),
                "maxWallet", capitalConfig.maxWallet(),
                "microLiveCapital", microCapital,
                "capitalAsset", capitalConfig.capitalAsset(),
                "capitalConfigReasonCode", "CAPITAL_CONFIG_FOUND_FROM_USER_DETAIL",
                "sourceSymbol", symbolResolution == null ? null : symbolResolution.sourceSymbol(),
                "targetSymbol", symbolResolution == null ? null : symbolResolution.targetSymbol()
        )), detail, evidence, symbolResolution, microCapital);
    }

    private PromotionDecision evaluateEvidence(ShadowCopyAllocationEntity shadow, Evidence evidence, OffsetDateTime now) {
        boolean summaryOnlyBlockedBySummary = summaryOnlyBlockedBySummary(shadow);
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
        BigDecimal minCoverage = nullToZero(properties.getMinShadowCoveragePct());
        if (minCoverage.compareTo(ZERO) > 0 && evidence.coveragePct().compareTo(minCoverage) < 0) {
            return PromotionDecision.rejected("SHADOW_NOT_READY_COVERAGE", details(shadow, evidence, Map.of()));
        }
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

    private PromotionOutput promote(ShadowCopyAllocationEntity shadow, PromotionDecision decision, OffsetDateTime now) {
        DetailUserEntity detail = Objects.requireNonNull(decision.detail(), "detail");
        BigDecimal allocationPct = firstPositive(shadow.getTargetLiveAllocationPct(), shadow.getAllocationPct(), new BigDecimal("0.000001"));
        BigDecimal microCapital = decision.microLiveCapital();

        PlanResolution planResolution = resolveCopyPlan(shadow, detail, decision, now, allocationPct, microCapital);
        UserWalletCopyPlanEntity plan = planResolution.plan();
        decision.details().put("copyPlanReasonCode", planResolution.reasonCode());

        CopySymbolResolution symbol = decision.symbolResolution();
        UserCopyAllocationEntity allocation = UserCopyAllocationEntity.builder()
                .idUser(shadow.getIdUser())
                .walletId(normalizeWallet(shadow.getWalletId()))
                .copyStrategyCode(normalizeStrategy(shadow.getCopyStrategyCode()))
                .copyStrategySlug(shadow.getCopyStrategySlug())
                .copyStrategyLabel(shadow.getCopyStrategyLabel())
                .copyMode(shadow.getCopyMode())
                .strategySourceEndpoint(shadow.getStrategySourceEndpoint())
                .rankWithinStrategy(shadow.getRankWithinStrategy())
                .globalRank(shadow.getGlobalRank())
                .strategyScore(shadow.getStrategyScore())
                .allocationPct(allocationPct)
                .score(shadow.getDecisionScore())
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .executionMode("MICRO_LIVE")
                .statusReason("PROMOTED_FROM_SHADOW")
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
        decision.details().put("microLiveAllocationReasonCode", "MICRO_LIVE_ALLOCATION_CREATED");

        shadow.setLinkedLiveAllocationId(allocation.getId());
        shadow.setPromotedToLiveAt(now);
        shadow.setStatus("PROMOTED_TO_MICRO_LIVE");
        shadow.setLastValidationReason("SHADOW_VALIDATED_READY_FOR_MICRO");
        shadow.setUpdatedAt(now);
        shadowAllocationRepository.saveAndFlush(shadow);
        return new PromotionOutput(plan, allocation);
    }

    private PlanResolution resolveCopyPlan(
            ShadowCopyAllocationEntity shadow,
            DetailUserEntity detail,
            PromotionDecision decision,
            OffsetDateTime now,
            BigDecimal allocationPct,
            BigDecimal microCapital
    ) {
        String walletLc = normalizeWallet(shadow.getWalletId());
        Optional<UserWalletCopyPlanEntity> existing = planRepository.findByIdUserAndWalletLc(shadow.getIdUser(), walletLc);
        boolean alreadyExists = existing.isPresent();
        UserWalletCopyPlanEntity plan = existing.orElseGet(() -> UserWalletCopyPlanEntity.builder()
                .idUser(shadow.getIdUser())
                .walletLc(walletLc)
                .createdAt(now)
                .build());
        applyPlanFields(plan, shadow, detail, decision, now, allocationPct, microCapital);
        try {
            plan = planRepository.saveAndFlush(plan);
            String reasonCode = alreadyExists ? "COPY_PLAN_ALREADY_EXISTS" : "COPY_PLAN_CREATED";
            logCopyPlanResolved(shadow, plan, reasonCode, alreadyExists ? "REUSED" : "CREATED");
            return new PlanResolution(plan, reasonCode);
        } catch (DataIntegrityViolationException ex) {
            if (alreadyExists) {
                throw ex;
            }
            Optional<UserWalletCopyPlanEntity> concurrent = planRepository.findByIdUserAndWalletLc(shadow.getIdUser(), walletLc);
            if (concurrent.isEmpty()) {
                throw ex;
            }
            logCopyPlanResolved(shadow, concurrent.get(), "COPY_PLAN_ALREADY_EXISTS", "REUSED");
            return new PlanResolution(concurrent.get(), "COPY_PLAN_ALREADY_EXISTS");
        }
    }

    private void applyPlanFields(
            UserWalletCopyPlanEntity plan,
            ShadowCopyAllocationEntity shadow,
            DetailUserEntity detail,
            PromotionDecision decision,
            OffsetDateTime now,
            BigDecimal allocationPct,
            BigDecimal microCapital
    ) {
        plan.setAllocationPct(allocationPct);
        plan.setScore(shadow.getDecisionScore());
        plan.setStatus("ACTIVE");
        plan.setActive(true);
        plan.setMetricVersion(1);
        plan.setMaxWallet(detail.getMaxWallet());
        plan.setUserCapitalUsd(BigDecimal.valueOf(Math.max(0, detail.getCapital() == null ? 0 : detail.getCapital())).setScale(8, RoundingMode.HALF_UP));
        plan.setAllocatedCapitalUsd(microCapital.setScale(8, RoundingMode.HALF_UP));
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

    private void logCopyPlanResolved(ShadowCopyAllocationEntity shadow, UserWalletCopyPlanEntity plan, String reasonCode, String decision) {
        String event = "COPY_PLAN_CREATED".equals(reasonCode)
                ? "copy.promotion.copy_plan.created"
                : "copy.promotion.copy_plan.reused";
        log.info(
                "event={} userId={} walletId={} shadowAllocationId={} planId={} strategyCode={} scopeType={} scopeValue={} reasonCode={} executionMode=MICRO_LIVE decision={}",
                event,
                shadow.getIdUser(),
                shadow.getWalletId(),
                shadow.getId(),
                plan == null ? null : plan.getId(),
                shadow.getCopyStrategyCode(),
                shadow.getScopeType(),
                shadow.getScopeValue(),
                reasonCode,
                decision
        );
    }

    private Evidence evidence(ShadowCopyAllocationEntity shadow, ShadowWalletProfileValidationEntity validation, OffsetDateTime now) {
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
        return new Evidence(days, events, skipped, errors, closed, pnl, drawdown, coverage);
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
        return allocationRepository.findOpenAllocationForUserWalletStrategyScope(
                shadow.getIdUser(),
                shadow.getWalletId(),
                normalizeStrategy(shadow.getCopyStrategyCode()),
                normalizeScopeType(shadow.getScopeType()),
                normalizeScopeValue(shadow.getScopeValue(), shadow.getCopyStrategyCode())
        );
    }

    private void linkShadowToExisting(ShadowCopyAllocationEntity shadow, UserCopyAllocationEntity allocation, OffsetDateTime now) {
        if (shadow == null || allocation == null) {
            return;
        }
        shadow.setLinkedLiveAllocationId(allocation.getId());
        shadow.setPromotedToLiveAt(firstNonNull(shadow.getPromotedToLiveAt(), allocation.getPromotedFromShadowAt(), now));
        shadow.setStatus("PROMOTED_TO_MICRO_LIVE");
        shadow.setLastValidationReason("ALREADY_PROMOTED");
        shadow.setUpdatedAt(now);
        shadowAllocationRepository.save(shadow);
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
                .targetExecutionMode(decision.allowed() ? "MICRO_LIVE" : "SHADOW")
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
        }
        if (extra != null) {
            extra.forEach((key, value) -> {
                if (value != null) details.put(key, value);
            });
        }
        return details;
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
        if (!"SUMMARY_NOT_FINAL_LIVE_BLOCKED".equals(normalizeToken(shadow == null ? null : shadow.getLastValidationReason()))) {
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
        return normalizeWallet(shadow.getWalletId())
                + "|" + normalizeStrategy(shadow.getCopyStrategyCode())
                + "|" + normalizeScopeType(shadow.getScopeType())
                + "|" + normalizeScopeValue(shadow.getScopeValue(), shadow.getCopyStrategyCode());
    }

    private static BigDecimal firstPositive(BigDecimal... values) {
        if (values != null) {
            for (BigDecimal value : values) {
                if (value != null && value.compareTo(ZERO) > 0) {
                    return value.setScale(6, RoundingMode.HALF_UP);
                }
            }
        }
        return ZERO.setScale(6, RoundingMode.HALF_UP);
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
        if (value == null || value.isBlank()) return "strategy";
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private static String normalizeScopeValue(String value, String strategy) {
        if (value == null || value.isBlank()) return normalizeStrategy(strategy);
        return value.trim();
    }

    private static String normalizeToken(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
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

    private static long elapsedMs(long startedNs) {
        return Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNs)).toMillis();
    }

    private record PromotionOutput(
            UserWalletCopyPlanEntity plan,
            UserCopyAllocationEntity allocation
    ) {
    }

    private record PlanResolution(
            UserWalletCopyPlanEntity plan,
            String reasonCode
    ) {
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

    private record Evidence(
            long shadowDays,
            long events,
            long skipped,
            long errors,
            long closedPositions,
            BigDecimal netPnlUsd,
            BigDecimal maxDrawdownPct,
            BigDecimal coveragePct
    ) {
        private Evidence {
            netPnlUsd = nullToZero(netPnlUsd);
            maxDrawdownPct = nullToZero(maxDrawdownPct);
            coveragePct = nullToZero(coveragePct);
        }
    }

    private record PromotionDecision(
            boolean allowed,
            String reasonCode,
            Map<String, Object> details,
            DetailUserEntity detail,
            Evidence evidence,
            CopySymbolResolution symbolResolution,
            BigDecimal microLiveCapital
    ) {
        private static PromotionDecision allowed(
                Map<String, Object> details,
                DetailUserEntity detail,
                Evidence evidence,
                CopySymbolResolution symbolResolution,
                BigDecimal microLiveCapital
        ) {
            return new PromotionDecision(true, "SHADOW_VALIDATED_READY_FOR_MICRO", details, detail, evidence, symbolResolution, nullToZero(microLiveCapital));
        }

        private static PromotionDecision rejected(String reasonCode, Map<String, Object> details) {
            return new PromotionDecision(false, reasonCode, details, null, null, null, ZERO);
        }
    }
}
