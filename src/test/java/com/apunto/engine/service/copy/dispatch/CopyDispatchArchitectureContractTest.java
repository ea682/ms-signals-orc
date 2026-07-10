package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

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
