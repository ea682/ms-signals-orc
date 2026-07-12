package com.apunto.engine.service.copy.reliability;

import com.apunto.engine.repository.ShadowCoverageCountsProjection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyTradingShadowRuntimeReliabilityContractTest {

    @Test
    void rollingProjectionUsesHibernateNativeTemporalType() throws NoSuchMethodException {
        assertEquals(Instant.class,
                ShadowCoverageCountsProjection.class.getMethod("getOldestEventTime").getReturnType());
        assertEquals(Instant.class,
                ShadowCoverageCountsProjection.class.getMethod("getNewestEventTime").getReturnType());
    }

    @Test
    void rollingCoveragePublishesStartedSucceededFailedAndFallbackContracts() throws IOException {
        String service = source("src/main/java/com/apunto/engine/service/copy/coverage/PostgresShadowCoverageQueryService.java");

        assertTrue(service.contains("event=shadow.coverage.query.started"));
        assertTrue(service.contains("event=shadow.coverage.query.succeeded"));
        assertTrue(service.contains("event=shadow.coverage.query.failed"));
        assertTrue(service.contains("event=shadow.coverage.fallback.used"));
        assertTrue(service.contains("sourceTemporalType=Instant targetTemporalType=OffsetDateTime"));
        assertTrue(service.contains("shadow_coverage_query_total"));
        assertTrue(service.contains("shadow_coverage_query_duration"));
        assertTrue(service.contains("shadow_coverage_fallback_total"));
    }

    @Test
    void distributionCoordinatorDoesNotOwnOneGlobalTransaction() throws IOException {
        String service = source("src/main/java/com/apunto/engine/service/impl/UserCopyAllocationServiceImpl.java");
        String executor = source("src/main/java/com/apunto/engine/service/copy/distribution/CopyDistributionUnitExecutor.java");
        Pattern globalTransaction = Pattern.compile(
                "@Override\\s+@Transactional\\s+public void syncDistribution", Pattern.MULTILINE);

        assertFalse(globalTransaction.matcher(service).find());
        assertTrue(service.contains("CopyDistributionUnitExecutor"));
        assertTrue(executor.contains("event=copy.distribution.unit.started"));
        assertTrue(executor.contains("event=copy.distribution.unit.completed"));
        assertTrue(executor.contains("event=copy.distribution.unit.failed"));
    }

    @Test
    void distributionLockUsesCanonicalProfileKeyInsteadOfTheLogSanitizedValue() throws IOException {
        String executor = source("src/main/java/com/apunto/engine/service/copy/distribution/CopyDistributionUnitExecutor.java");

        assertTrue(executor.contains("String canonicalProfileKey = canonicalProfileKey(profileKey);"));
        assertTrue(executor.contains("String lockKey = lockKey(userId, walletId, canonicalProfileKey);"));
        assertFalse(executor.contains("String lockKey = lockKey(userId, walletId, safeProfileKey);"));
    }

    @Test
    void shadowPromotionDecisionIsCalculatedBeforeThePerProfileTransaction() throws IOException {
        String service = source("src/main/java/com/apunto/engine/service/impl/ShadowCopyTradingServiceImpl.java");
        int decision = service.indexOf("ShadowCandidateDecision candidateDecision = evaluateShadowCandidateDecision(");
        int unitCall = service.indexOf("syncShadowDistributionUnit(", decision);
        int transactionCallback = service.indexOf("copyDistributionUnitExecutor.execute(", unitCall);
        int mutationMethod = service.indexOf("private ShadowSyncOutcome syncShadowCandidate(");
        int mutationMethodEnd = service.indexOf("private record ShadowSyncOutcome", mutationMethod);

        assertTrue(decision >= 0, "the promotion decision must be materialized outside the unit transaction");
        assertTrue(unitCall > decision);
        assertTrue(transactionCallback > unitCall);
        assertTrue(mutationMethod > transactionCallback);
        String mutationBody = service.substring(mutationMethod, mutationMethodEnd);
        assertFalse(mutationBody.contains("isLivePromotable("));
        assertFalse(mutationBody.contains("shadowValidationDecision("));
    }

    @Test
    void promotionMutationsUseTheSamePerProfileTransactionExecutor() throws IOException {
        String promotion = source("src/main/java/com/apunto/engine/service/impl/ShadowPromotionServiceImpl.java");

        assertTrue(promotion.contains("CopyDistributionUnitExecutor copyDistributionUnitExecutor"));
        assertTrue(promotion.contains("copyDistributionUnitExecutor.execute("));
        assertTrue(promotion.contains("promoteUnderProfileLock("));
        assertTrue(promotion.contains("markRejectedUnderProfileLock("));
        assertTrue(promotion.contains("linkShadowToExistingUnderProfileLock("));
    }

    @Test
    void deadlockRetryHasStructuredRetryAndExhaustionEvents() throws IOException {
        String retry = source("src/main/java/com/apunto/engine/service/copy/concurrency/PostgresDeadlockRetryExecutor.java");

        assertTrue(retry.contains("event=postgres.deadlock.retry"));
        assertTrue(retry.contains("event=postgres.deadlock.exhausted"));
        assertTrue(retry.contains("reasonCode=POSTGRES_DEADLOCK_RETRY"));
        assertTrue(retry.contains("reasonCode=POSTGRES_DEADLOCK_RETRIES_EXHAUSTED"));
    }

    @Test
    void shadowValidationUsesOneDecisionAggregate() throws IOException {
        String service = source("src/main/java/com/apunto/engine/service/impl/ShadowCopyTradingServiceImpl.java");
        String repository = source("src/main/java/com/apunto/engine/repository/ShadowCopyOperationEventRepository.java");

        assertFalse(service.contains("countByWalletProfileIdAndDecision("));
        assertTrue(service.contains("summarizeDecisionsByWalletProfileId("));
        assertTrue(repository.contains("summarizeDecisionsByWalletProfileId"));
        assertTrue(repository.contains("count(*) filter"));
    }

    @Test
    void skippedShadowEventsDoNotRecomputeTheWholeValidationSnapshot() throws IOException {
        String service = source("src/main/java/com/apunto/engine/service/impl/ShadowCopyTradingServiceImpl.java");
        int method = service.indexOf("private boolean shouldRefreshProfileValidation(");
        int nextMethod = service.indexOf("private static OffsetDateTime maxTime", method);
        String body = service.substring(method, nextMethod);

        assertTrue(body.contains("!impact.positionUpdated()"));
        assertTrue(body.contains("return false;"));
    }

    @Test
    void shadowQueueDelayIsCapturedAtDequeueBeforeProcessing() throws IOException {
        String ingest = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectDeltaIngestServiceImpl.java");
        int dequeue = ingest.indexOf("long shadowDequeuedNs = System.nanoTime()");
        int queueDelay = ingest.indexOf("long queueDelayMs = elapsedMs(task.acceptedNs(), shadowDequeuedNs)");
        int processing = ingest.indexOf("recordShadowWithDeadlockRetry", queueDelay);

        assertTrue(dequeue >= 0);
        assertTrue(queueDelay > dequeue);
        assertTrue(processing > queueDelay);
        assertTrue(ingest.contains("shadow_queue_delay"));
        assertTrue(ingest.contains("event=shadow.processing.slow"));
    }

    @Test
    void shadowDbPersistTimerIncludesTheFinalJpaFlush() throws IOException {
        String shadow = source("src/main/java/com/apunto/engine/service/impl/ShadowCopyTradingServiceImpl.java");
        int dbTimerStart = shadow.indexOf("long dbNs = timing.mark()");
        int eventSave = shadow.indexOf("shadowEventRepository.save(shadowEvent)", dbTimerStart);
        int finalFlush = shadow.indexOf("shadowEventRepository.flush()", eventSave);
        int dbTimerStop = shadow.indexOf("timing.add(CopyFlowTiming.Stage.DB_PERSIST", finalFlush);

        assertTrue(dbTimerStart >= 0);
        assertTrue(eventSave > dbTimerStart);
        assertTrue(finalFlush > eventSave);
        assertTrue(dbTimerStop > finalFlush);
    }

    @Test
    void liveMetricLookupUsesFullAllocationKeyIncludingScope() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");
        String guard = source("src/main/java/com/apunto/engine/service/copy/CopyStrategyGuardRuntimeCache.java");

        assertTrue(engine.contains("metricsByAllocationKey"));
        assertFalse(engine.contains("metricsByStrategy"));
        assertTrue(guard.contains("String scopeType"));
        assertTrue(guard.contains("String scopeValue"));
    }

    @Test
    void microLiveExitsAndLiveDeclareBudgetBypass() throws IOException {
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");

        assertTrue(store.contains("budgetCheck=SKIPPED_FOR_REDUCE_OR_CLOSE"));
        assertTrue(store.contains("reasonCode=MICRO_LIVE_EXIT_ALWAYS_ALLOWED"));
        assertTrue(store.contains("budgetMode=LIVE_UNRESTRICTED_BY_MICRO_LIMITS"));
        assertTrue(store.contains("microLiveBudgetLockAcquired=false"));
    }

    @Test
    void resizeAfterClosedLogsOrderingContextAndPublishesMetric() throws IOException {
        String shadow = source("src/main/java/com/apunto/engine/service/impl/ShadowCopyTradingServiceImpl.java");

        assertTrue(shadow.contains("event=shadow.resize.after_closed"));
        assertTrue(shadow.contains("lastAcceptedEventType"));
        assertTrue(shadow.contains("lastAcceptedEventAt"));
        assertTrue(shadow.contains("incomingEventAt"));
        assertTrue(shadow.contains("orderingDecision"));
        assertTrue(shadow.contains("resize_after_shadow_closed_total"));
    }

    @Test
    void distributedDedupeDistinguishesPayloadConflictFromHealthyReplay() throws IOException {
        String guard = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectIngestIdempotencyGuard.java");

        assertTrue(guard.contains("IDEMPOTENCY_KEY_PAYLOAD_CONFLICT"));
        assertTrue(guard.contains("payloadFingerprint"));
        assertTrue(guard.contains("DISTRIBUTED_DUPLICATE_SUPPRESSED"));
    }

    @Test
    void shadowQueueOverflowBecomesDurableAfterLiveDispatchBeforeIngestCompletion() throws IOException {
        String ingest = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectDeltaIngestServiceImpl.java");
        String replay = source("src/main/java/com/apunto/engine/service/copy/recovery/ShadowEventDeadLetterReplayWorker.java");
        int liveDispatch = ingest.indexOf("directCopyDispatchService.dispatch");
        int durableRecovery = ingest.indexOf("if (shadowEnqueue.recoveryRequired())");
        int ingestCompleted = ingest.indexOf("idempotencyGuard.markProcessed", durableRecovery);

        assertTrue(liveDispatch >= 0);
        assertTrue(durableRecovery > liveDispatch);
        assertTrue(ingestCompleted > durableRecovery);
        assertTrue(ingest.contains("SHADOW_RECOVERY_PERSIST_FAILED_AFTER_LIVE_DISPATCH"));
        assertTrue(replay.contains("claimRecoverable"));
        assertTrue(replay.contains("markResolved"));
        assertTrue(replay.contains("markReplayFailed"));
    }

    @Test
    void everyShadowWorkerFailureAttemptsDurableRecovery() throws IOException {
        String ingest = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectDeltaIngestServiceImpl.java");
        int methodStart = ingest.indexOf("private void processShadowTask");
        int failureStart = ingest.indexOf("catch (RuntimeException ex)", methodStart);
        int failureEnd = ingest.indexOf("} finally", failureStart);
        String failureBlock = ingest.substring(failureStart, failureEnd);

        assertTrue(failureBlock.contains("persistRecoverableShadowFailure(mappedDelta"));
        assertFalse(failureBlock.contains("if (deadlockExhausted)"));
    }

    @Test
    void queueRejectionIsDeferredInsteadOfCountedAsLostBeforeDurableRecovery() throws IOException {
        String ingest = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectDeltaIngestServiceImpl.java");
        int enqueueStart = ingest.indexOf("private ShadowEnqueueResult enqueueShadowBeforeLive");
        int enqueueEnd = ingest.indexOf("private ShadowSyncResult recordShadowSynchronously", enqueueStart);
        String enqueueMethod = ingest.substring(enqueueStart, enqueueEnd);

        assertFalse(enqueueMethod.contains("shadowDropped.incrementAndGet()"));
        assertTrue(ingest.contains("shadowDeferred.incrementAndGet()"));
        assertTrue(ingest.contains("reasonCode=SHADOW_RECOVERY_PERSIST_FAILED_AFTER_LIVE_DISPATCH"));
    }

    @Test
    void synchronousShadowRecoveryFailureIsPropagatedToThePostLiveDurabilityGate() throws IOException {
        String ingest = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectDeltaIngestServiceImpl.java");

        assertTrue(ingest.contains("record ShadowSyncResult("));
        assertTrue(ingest.contains("syncResult.recoveryRequired()"));
        assertTrue(ingest.contains("syncResult.durableDeferred()"));
        assertFalse(ingest.contains("private int recordShadowSynchronously"));
    }

    @Test
    void concurrentAggregateIndexRecoversAnInvalidPartialBuild() throws IOException {
        String migration = source(
                "src/main/resources/db/migration/V202607110004__shadow_event_profile_decision_aggregate_index.sql");
        int drop = migration.indexOf("DROP INDEX CONCURRENTLY IF EXISTS");
        int create = migration.indexOf("CREATE INDEX CONCURRENTLY");

        assertTrue(drop >= 0);
        assertTrue(create > drop);
        assertFalse(migration.contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS"));
    }

    @Test
    void shadowPositionStatePersistsTheLastAcceptedSourceEventTime() throws IOException {
        String entity = source("src/main/java/com/apunto/engine/entity/ShadowPositionStateEntity.java");
        String migration = source(
                "src/main/resources/db/migration/V202607110005__shadow_position_last_accepted_event_at.sql");

        assertTrue(entity.contains("private OffsetDateTime lastAcceptedEventAt;"));
        assertTrue(entity.contains("name = \"last_accepted_event_at\""));
        assertTrue(migration.contains("ADD COLUMN IF NOT EXISTS last_accepted_event_at timestamptz"));
    }

    private String source(String path) throws IOException {
        Path file = Path.of(path);
        assertTrue(Files.isRegularFile(file), "missing source: " + file.toAbsolutePath());
        return Files.readString(file);
    }
}
