package com.apunto.engine.service.impl;

import com.apunto.engine.entity.CopyPromotionAuditEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.CopyOperationEventRepository;
import com.apunto.engine.repository.CopyPromotionAuditRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.MicroLivePromotionService;
import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import com.apunto.engine.service.copy.promotion.LivePromotionResult;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver.CopyModeResolution;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class MicroLivePromotionServiceImpl implements MicroLivePromotionService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal HUNDRED = new BigDecimal("100");

    private final UserCopyAllocationRepository allocationRepository;
    private final CopyOperationEventRepository eventRepository;
    private final CopyPromotionAuditRepository auditRepository;
    private final LivePromotionProperties properties;

    @Override
    @Transactional
    public LivePromotionResult promoteMicroLiveToLive() {
        if (properties == null || !properties.isEnabled()) {
            log.info("event=copy.promotion.micro_to_live.skipped reason=PROMOTION_DISABLED");
            return LivePromotionResult.empty();
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
        log.info("event=copy.promotion.micro_to_live.started candidates={}", candidates.size());

        for (UserCopyAllocationEntity allocation : candidates) {
            if (allocation == null || allocation.getId() == null) {
                skipped++;
                continue;
            }
            evaluated++;
            try {
                LiveDecision decision = evaluate(allocation, now);
                audit(allocation, decision, "MICRO_LIVE_EVALUATED");
                if (!decision.allowed() && "LIVE_ALLOCATION_ALREADY_EXISTS".equals(decision.reasonCode())) {
                    skipped++;
                    audit(allocation, decision, "MICRO_LIVE_PROMOTION_NOOP");
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
                    allocation.setStatusReason(decision.reasonCode());
                    allocation.setStatusUpdatedAt(now);
                    allocation.setUpdatedAt(now);
                    allocationRepository.save(allocation);
                    audit(allocation, decision, "MICRO_LIVE_PROMOTION_REJECTED");
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
                allocation.setStatus(UserCopyAllocationEntity.Status.CLOSED);
                allocation.setActive(false);
                allocation.setStatusReason("PROMOTED_MICRO_TO_LIVE_CLOSED");
                allocation.setStatusUpdatedAt(now);
                allocation.setUpdatedAt(now);
                UserCopyAllocationEntity closedMicro = allocationRepository.saveAndFlush(allocation);

                UserCopyAllocationEntity liveAllocation = allocationRepository.saveAndFlush(liveFromMicro(closedMicro, now, decision.resolvedCopyMode()));
                promoted++;
                auditMicroClosed(closedMicro, decision);
                auditLiveCreated(closedMicro, liveAllocation, decision);
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
            } catch (DataIntegrityViolationException ex) {
                Optional<UserCopyAllocationEntity> existing = existingLive(allocation);
                if (existing.isPresent()) {
                    skipped++;
                    LiveDecision noop = rejected("LIVE_ALLOCATION_ALREADY_EXISTS", allocation, 0, 0, ZERO, ZERO, 0, safe(existing.get().getCopyMode()));
                    audit(allocation, noop, "MICRO_LIVE_PROMOTION_NOOP");
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
                audit(allocation, failed, "MICRO_LIVE_PROMOTION_REJECTED");
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
                audit(allocation, failed, "MICRO_LIVE_PROMOTION_REJECTED");
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

    private UserCopyAllocationEntity liveFromMicro(UserCopyAllocationEntity micro, OffsetDateTime now, String resolvedCopyMode) {
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
                .allocationPct(micro.getAllocationPct())
                .score(micro.getScore())
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .updatedAt(now)
                .isActive(true)
                .executionMode("LIVE")
                .statusReason("PROMOTED_MICRO_TO_LIVE")
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
                .sourceRankingVersion(micro.getSourceRankingVersion())
                .sourceSymbol(micro.getSourceSymbol())
                .targetSymbol(micro.getTargetSymbol())
                .capitalAsset(micro.getCapitalAsset())
                .resolvedQuoteAsset(micro.getResolvedQuoteAsset())
                .symbolResolutionStatus(micro.getSymbolResolutionStatus())
                .symbolResolutionReason(micro.getSymbolResolutionReason())
                .build();
    }

    private LiveDecision evaluate(UserCopyAllocationEntity allocation, OffsetDateTime now) {
        if (!"MICRO_LIVE".equals(UserCopyAllocationEntity.normalizeExecutionMode(allocation.getExecutionMode()))) {
            return rejected("MICRO_LIVE_NOT_READY_WRONG_MODE", allocation, 0, 0, ZERO, ZERO, 0);
        }
        if (allocation.getIdUser() == null || allocation.getWalletId() == null) {
            return rejected("MICRO_LIVE_NOT_READY_INVALID_ALLOCATION", allocation, 0, 0, ZERO, ZERO, 0);
        }
        Optional<UserCopyAllocationEntity> existingLive = existingLive(allocation);
        if (existingLive.isPresent()) {
            return rejected("LIVE_ALLOCATION_ALREADY_EXISTS", allocation, 0, 0, ZERO, ZERO, 0, existingLive.get().getCopyMode());
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

        long events = eventRepository.countRuntimeEventsForAllocation(allocation.getId(), "MICRO_LIVE");
        long errors = eventRepository.countRuntimeErrorEventsForAllocation(allocation.getId(), "MICRO_LIVE");
        BigDecimal pnl = nullToZero(eventRepository.sumRuntimeRealizedPnlUsdForAllocation(allocation.getId(), "MICRO_LIVE"));
        BigDecimal errorRate = errorRatePct(events, errors);
        OffsetDateTime firstEventAt = eventRepository.findFirstRuntimeEventTimeForAllocation(allocation.getId(), "MICRO_LIVE");
        OffsetDateTime since = firstNonNull(firstEventAt, allocation.getPromotedFromShadowAt(), allocation.getUpdatedAt(), now);
        long days = Math.max(0L, Duration.between(since, now).toDays());

        if (days < Math.max(0, properties.getMinMicroDays())) {
            return rejected("MICRO_LIVE_NOT_READY_MIN_DAYS", allocation, events, errors, errorRate, pnl, days);
        }
        if (events < Math.max(0, properties.getMinMicroOrders())) {
            return rejected("MICRO_LIVE_NOT_READY_MIN_ORDERS", allocation, events, errors, errorRate, pnl, days);
        }
        BigDecimal maxErrorRate = nullToZero(properties.getMaxErrorRatePct());
        if (maxErrorRate.compareTo(ZERO) >= 0 && errorRate.compareTo(maxErrorRate) > 0) {
            return rejected("MICRO_LIVE_NOT_READY_ERROR_RATE", allocation, events, errors, errorRate, pnl, days);
        }
        if (properties.isRequirePositiveNetPnl()
                && pnl.compareTo(nullToZero(properties.getMinNetPnlUsd())) < 0) {
            return rejected("MICRO_LIVE_NOT_READY_NEGATIVE_PNL", allocation, events, errors, errorRate, pnl, days);
        }

        return new LiveDecision(
                true,
                "MICRO_LIVE_VALIDATED_READY_FOR_LIVE",
                details(allocation, events, errors, errorRate, pnl, days, copyModeResolution.copyMode(), copyModeResolution.reasonCode(), copyModeResolution.constraintReasonCode()),
                events,
                errors,
                errorRate,
                pnl,
                days,
                copyModeResolution.copyMode()
        );
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
            String resolvedCopyMode
    ) {
        private LiveDecision {
            reasonCode = reasonCode == null ? "UNKNOWN" : reasonCode.trim().toUpperCase(Locale.ROOT).replace('-', '_');
            errorRatePct = nullToZero(errorRatePct);
            netPnlUsd = nullToZero(netPnlUsd);
        }
    }
}
