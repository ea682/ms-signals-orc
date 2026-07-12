package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDispatchArchitectureContractTest {

    @Test
    void liveHotPathDoesNotCallMetricService() throws IOException {
        String dispatchSource = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyDispatchCoordinator.java");
        String resolverSource = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidCopyCandidateResolver.java");

        assertFalse(dispatchSource.contains("MetricWalletService"));
        assertFalse(dispatchSource.contains("MetricService"));
        assertFalse(resolverSource.contains("MetricWalletService"));
        assertTrue(Arrays.stream(CopyDispatchCoordinator.class.getDeclaredFields())
                .noneMatch(field -> field.getType().getSimpleName().toLowerCase().contains("metric")));
    }

    @Test
    void liveHotPathDoesNotCallFullSimulation() throws IOException {
        String dispatchSource = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyDispatchCoordinator.java");
        String resolverSource = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidCopyCandidateResolver.java");
        String normalized = (dispatchSource + resolverSource).toLowerCase();

        assertFalse(normalized.contains("simulation=full"));
        assertFalse(normalized.contains("fullsimulation"));
        assertFalse(normalized.contains("simulationfull"));
    }

    @Test
    void microLiveBudgetSnapshotUsesOneAggregateDatabaseCall() throws IOException {
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");

        assertTrue(store.contains("repository.loadBudgetSnapshot("));
        assertFalse(store.contains("sumActiveMarginForAllocation("));
        assertFalse(store.contains("sumPendingReservedMargin("));
        assertFalse(store.contains("countActiveForAllocation("));
        assertFalse(store.contains("sumPendingReservedPositions("));
    }

    @Test
    void microLiveBudgetIsSharedByUserAndWalletAcrossStrategies() throws IOException {
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");
        String repository = source("src/main/java/com/apunto/engine/repository/CopyDispatchIntentRepository.java");

        assertTrue(store.contains("request.walletId()"));
        assertTrue(repository.contains("lower(co.id_wallet_origin) = lower(:walletId)"));
        assertTrue(repository.contains("lower(cdi.wallet_id) = lower(:walletId)"));
        assertFalse(repository.contains("co.user_copy_allocation_id = :allocationId"));
    }

    @Test
    void microLiveBudgetExhaustionDoesNotBlockReductionsOrCloses() throws IOException {
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");

        assertTrue(store.contains("&& !request.reduceOnly()"));
        assertTrue(store.contains("requiresMicroLiveBudgetLock(request)"));
        assertTrue(store.contains("if (requiresMicroLiveBudget)"));
    }

    @Test
    void microLiveBudgetDecisionLogContainsTheCompleteAuditContract() throws IOException {
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");

        assertTrue(store.contains("event=copy.budget.evaluated"));
        for (String field : List.of(
                "reasonCode={}", "decision={}", "result={}", "executionMode=MICRO_LIVE",
                "userId={}", "walletId={}", "strategyCode={}", "allocationId={}",
                "sourceEventId={}", "idempotencyKey={}", "clientOrderId={}",
                "requestedMarginUsd={}", "reservedMarginUsd={}", "walletOpenMarginUsd={}",
                "walletPendingMarginUsd={}", "walletRemainingMarginUsd={}",
                "openPositionCount={}", "pendingPositionCount={}",
                "maxWalletMarginUsd={}", "maxMarginPerOperationUsd={}",
                "maxConcurrentPositions={}", "lockWaitMs={}",
                "microLiveBudgetLockAcquired=true")) {
            assertTrue(store.contains(field), "missing budget audit field: " + field);
        }
    }

    @Test
    void microLiveOpenBlocksExposeTheCompleteStableReasonCatalog() throws IOException {
        String sources = String.join("\n",
                source("src/main/java/com/apunto/engine/service/copy/budget/CopyBudgetResolver.java"),
                source("src/main/java/com/apunto/engine/service/copy/dispatch/MicroLiveBudgetPolicy.java"),
                source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyDispatchCoordinator.java"),
                source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidCopyCandidateResolver.java"),
                source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java"));

        for (String reasonCode : List.of(
                "MICRO_LIVE_TOTAL_MARGIN_EXCEEDED",
                "MICRO_LIVE_MAX_MARGIN_PER_OPERATION_EXCEEDED",
                "MICRO_LIVE_MAX_CONCURRENT_POSITIONS_EXCEEDED",
                "MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE",
                "MICRO_LIVE_DUPLICATE_INTENT",
                "MICRO_LIVE_GUARD_BLOCKED",
                "MICRO_LIVE_SYMBOL_NOT_ALLOWED",
                "MICRO_LIVE_MIN_NOTIONAL_NOT_REACHED")) {
            assertTrue(sources.contains(reasonCode), "missing MICRO_LIVE reason: " + reasonCode);
        }
        assertFalse(sources.contains("COPY_DISPATCH_ALREADY_REJECTED"));
    }

    @Test
    void microLiveHotPathSizingAggregatesTheWholeWalletAcrossStrategies() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");

        assertTrue(engine.contains("budgetScopeMatches(op, allocation, executionMode)"));
        assertTrue(engine.contains("if (\"MICRO_LIVE\".equals(executionMode))"));
    }

    @Test
    void readinessKeepsExhaustedAmbiguousOrdersClassifiedAfterManualReview() throws IOException {
        String repository = source("src/main/java/com/apunto/engine/repository/CopyDispatchIntentRepository.java");

        assertTrue(repository.contains("i.status IN ('RECONCILING', 'MANUAL_REVIEW')"));
        assertTrue(repository.contains("'NEW_ORDER_RECONCILIATION_EXHAUSTED'"));
        assertTrue(repository.contains("'ORDER_NOT_FOUND_REQUIRES_MANUAL_REVIEW'"));
    }

    @Test
    void candidateResolutionUsesCacheOnlySnapshots() throws IOException {
        String resolver = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidCopyCandidateResolver.java");

        assertTrue(resolver.contains("getUsersCachedOnly()"));
        assertTrue(resolver.contains("getActiveAllocationsByWalletCachedOnly("));
        assertFalse(resolver.contains("userDetailCachedService.getUsers()"));
        assertFalse(resolver.contains("userCopyAllocationService.getActiveAllocationsByWallet(walletId)"));
    }

    @Test
    void binanceSizingContextUsesCacheOnlyMetricAndAllocationSnapshots() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");

        assertTrue(engine.contains("getCandidatesForUserWalletCachedOnly("));
        assertTrue(engine.contains("getActiveAllocationsForUserWalletCachedOnly("));
        assertFalse(engine.contains("metricWalletService.getCandidatesUser("));
        assertFalse(engine.contains("userCopyAllocationService.getActiveAllocationsForUserWallet("));
    }

    @Test
    void persistedIntentRequiresRequiredLedgerLink() throws IOException {
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");
        String persistence = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyExecutionPersistenceService.java");

        assertTrue(store.contains("setCopyOperationEventId"));
        assertTrue(store.contains("COPY_REQUIRED_LEDGER_LINK_MISSING"));
        assertTrue(persistence.contains("linkRequiredEvent"));
    }

    @Test
    void requiredLedgerSerializesIdenticalProgressBeforeIdempotencyPrecheck() throws IOException {
        String service = source("src/main/java/com/apunto/engine/service/impl/CopyOperationEventServiceImpl.java");
        int lock = service.indexOf("repository.lockDispatchProgress(");
        int precheck = service.indexOf("repository.findDispatchProgress(");

        assertTrue(lock >= 0, "required ledger progress must take a transaction-scoped lock");
        assertTrue(precheck > lock, "the lock must be acquired before the idempotency precheck");
    }

    @Test
    void reconciliationUsesExplicitManualReviewState() throws IOException {
        String reconciliation = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyOrderReconciliationService.java");
        String migration = source("src/main/resources/db/migration/V202607100002__copy_dispatch_manual_review_integrity.sql");

        assertTrue(reconciliation.contains("MANUAL_REVIEW"));
        assertFalse(reconciliation.contains("setStatus(\"FAILED_FINAL\")"));
        assertTrue(migration.contains("'MANUAL_REVIEW'"));
    }

    @Test
    void reconciliationPublishesRequiredLowCardinalityOutcomeCounters() throws IOException {
        String worker = source("src/main/java/com/apunto/engine/jobs/CopyOrderReconciliationWorker.java");

        assertTrue(worker.contains("copy_reconciliation_success"));
        assertTrue(worker.contains("copy_reconciliation_failure"));
    }

    @Test
    void microLiveBudgetMigrationRecoversPartialConcurrentIndexes() throws IOException {
        String migration = source(
                "src/main/resources/db/migration/V202607110002__micro_live_wallet_budget.sql");
        String validation = source(
                "src/main/resources/db/validation/micro_live_wallet_budget_validation.sql");
        String application = source("src/main/resources/application.yml");

        assertTrue(migration.contains("SET lock_timeout = '60s'"));
        assertTrue(migration.contains(
                "DROP INDEX CONCURRENTLY IF EXISTS futuros_operaciones.ix_copy_dispatch_intent_micro_wallet_budget"));
        assertTrue(migration.contains(
                "DROP INDEX CONCURRENTLY IF EXISTS futuros_operaciones.ix_copy_operation_micro_wallet_budget"));
        assertFalse(migration.contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS"));
        assertTrue(migration.indexOf("DROP INDEX CONCURRENTLY IF EXISTS")
                < migration.indexOf("CREATE INDEX CONCURRENTLY"));

        assertTrue(validation.contains("expected_indexes(index_name)"));
        assertTrue(validation.contains("LEFT JOIN pg_index"));
        assertTrue(validation.contains("index_exists"));
        assertTrue(application.contains(
                "transactional-lock: ${FLYWAY_POSTGRESQL_TRANSACTIONAL_LOCK:false}"));
    }

    @Test
    void reservedPositionCountMatchesTheSmallintSchemaContract() throws IOException {
        String entity = source("src/main/java/com/apunto/engine/entity/CopyDispatchIntentEntity.java");
        String migration = source(
                "src/main/resources/db/migration/V202607090001__copy_dispatch_intent_idempotency.sql");

        assertTrue(migration.contains("reserved_position_count smallint NOT NULL DEFAULT 0"));
        assertTrue(entity.contains("private short reservedPositionCount;"));
        assertFalse(entity.contains("private int reservedPositionCount;"));
    }

    @Test
    void everyDurableStatusMutationIsGuardedByTheStatePolicy() throws IOException {
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");
        String reconciliation = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyOrderReconciliationService.java");

        assertTrue(occurrences(store, "CopyDispatchStatePolicy.requireTransition(") >= 5);
        assertTrue(reconciliation.contains("CopyDispatchStatePolicy.requireTransition("));
    }

    private int occurrences(String value, String token) {
        int count = 0;
        int from = 0;
        while ((from = value.indexOf(token, from)) >= 0) {
            count++;
            from += token.length();
        }
        return count;
    }

    private String source(String path) throws IOException {
        Path file = Path.of(path);
        assertTrue(Files.isRegularFile(file), "missing architecture source: " + file.toAbsolutePath());
        return Files.readString(file);
    }
}
