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
                "maxWalletMarginUsd={}", "userMaxConcurrentPositions={}",
                "lockWaitMs={}",
                "microLiveBudgetLockAcquired=true")) {
            assertTrue(store.contains(field), "missing budget audit field: " + field);
        }
        assertFalse(store.contains("maxMarginPerOperationUsd={}"));
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
                "SKIPPED_USER_POSITION_LIMIT",
                "MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE",
                "MICRO_LIVE_DUPLICATE_INTENT",
                "MICRO_LIVE_GUARD_BLOCKED",
                "MICRO_LIVE_SYMBOL_NOT_ALLOWED",
                "MICRO_LIVE_MIN_NOTIONAL_NOT_REACHED")) {
            assertTrue(sources.contains(reasonCode), "missing MICRO_LIVE reason: " + reasonCode);
        }
        assertFalse(sources.contains("MICRO_LIVE_MAX_MARGIN_PER_OPERATION_EXCEEDED"));
        assertFalse(sources.contains("MICRO_LIVE_MAX_CONCURRENT_POSITIONS_EXCEEDED"));
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
        assertTrue(repository.contains("'EXECUTION_TIMEOUT_RECONCILING'"));
        assertTrue(repository.contains("'EXECUTION_AMBIGUOUS_RECONCILING'"));
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
    void everyEntryAndRebalanceUsesThePurePortfolioCalculatorWithoutForcingBinanceMinimums() throws IOException {
        String pom = source("pom.xml");
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");

        assertTrue(pom.contains("<artifactId>copy-target-core</artifactId>"));
        assertTrue(engine.contains("TargetPortfolioCalculator"));
        assertTrue(occurrences(engine, "targetPortfolioCalculator.calculate(") >= 2,
                "direct entry and portfolio rebalance must share the pure calculator");
        assertFalse(engine.contains("adjustQuantityUpToMinNotional("));
        assertFalse(engine.contains("targetNotional = rules.effectiveMinNotional"));
        assertFalse(engine.contains("copyByMinNotional = true"));
        assertTrue(engine.contains("SKIPPED_BELOW_MIN_NOTIONAL"));
    }

    @Test
    void realSizingUsesAuthoritativeBinancePositionsWithoutStealingOtherAllocationExposure() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");
        String client = source("src/main/java/com/apunto/engine/client/BinanceClient.java");

        assertFalse(engine.contains(".existingPositions(List.of())"),
                "real sizing must not pretend the Binance account is flat");
        assertTrue(engine.contains(".managedExistingPositions("),
                "account-level managed exposure is required to detect manual/external positions");
        assertTrue(engine.contains(".portfolioExistingPositions("),
                "allocation-attributed exposure is required for allocation-safe deltas");
        assertTrue(engine.contains("BLOCKED_EXISTING_EXPOSURE_CONFLICT"));
        assertTrue(engine.contains("BLOCKED_TARGET_POSITION_SNAPSHOT_UNAVAILABLE"));
        assertTrue(client.contains("@GetExchange(\"/positions\")"));
        assertTrue(engine.contains("directTarget.waitsForOppositeClose()"),
                "the direct path must never open the opposite side before the close is confirmed");
        assertTrue(engine.contains("decision.sourceLegId().equals(originId)"),
                "the direct path must select its source leg explicitly instead of relying on list order");
    }

    @Test
    void externalManualCloseIsReconciledDurablyWithoutSyntheticEconomics() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");
        String ledger = source("src/main/java/com/apunto/engine/service/impl/CopyOperationEventServiceImpl.java");

        assertTrue(engine.contains("TargetPositionExitPolicy.resolve("));
        assertTrue(engine.contains("recordReconciliationRequired("));
        assertTrue(engine.contains("decision(\"RECONCILED_CLOSE\")"));
        assertTrue(engine.contains("copyIntent(\"EXTERNAL_CLOSE\")"));
        assertTrue(engine.contains("economicDataStatus(\"UNAVAILABLE\")"));
        assertTrue(engine.contains("priceClose(null)"));
        assertTrue(engine.contains("decision=NO_ORDER_LOCAL_CLOSE"));
        assertTrue(ledger.contains("persist(command, true, false)"),
                "external reconciliation must fail if its required ledger event cannot persist");
    }

    @Test
    void suspendedEntryEligibilityDoesNotEraseTheCapitalNeededToCalculateReductions() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");

        assertFalse(engine.contains("metricAllowsOpenOrResize ? resolveWalletBudget"),
                "an entry gate must not replace exit sizing capital with zero");
        assertTrue(occurrences(engine,
                "final BigDecimal walletBudget = resolveWalletBudget(userDetail, walletMetric, walletAllocation);") >= 2);
        assertTrue(occurrences(engine,
                "metricAllowsOpenOrResize && targetPlan.entrySizingAllowed()") >= 2,
                "the entry gate must still block OPEN and INCREASE after sizing");
    }

    @Test
    void sourceEquityHasNoSyntheticOneThousandDollarFallback() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");

        assertFalse(engine.contains("DEFAULT_BASE_CAPITAL"));
        assertFalse(engine.contains("BigDecimal.valueOf(1_000"));
        assertTrue(engine.contains("BLOCKED_SOURCE_EQUITY_MISSING"));
        assertTrue(engine.contains("BLOCKED_SOURCE_EQUITY_STALE"));
        assertTrue(engine.contains("BLOCKED_SOURCE_EQUITY_INVALID"));
    }

    @Test
    void flipAndFullCloseKeepRuntimeStateUntilZeroIsAuthoritativelyConfirmed() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");

        assertTrue(engine.contains("CloseExecutionStatus executeFullClose("));
        assertTrue(engine.contains("return CloseExecutionStatus.STILL_OPEN;"));
        assertTrue(engine.contains("return CloseExecutionStatus.CLOSED_CONFIRMED;"));
        assertTrue(occurrences(engine,
                "if (closeStatus == CloseExecutionStatus.CLOSED_CONFIRMED)") >= 6);
        assertFalse(engine.contains("executeFullClose(copy, userDetail);\n                    current.remove"));
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
    void v3OperationalValidationDoesNotReintroduceLegacyPerOrderOrGlobalPositionLimits() throws IOException {
        String validations = String.join("\n",
                source("src/main/resources/db/validation/micro_live_wallet_budget_validation.sql"),
                source("src/main/resources/db/validation/copy_dispatch_performance_validation.sql"));

        assertTrue(validations.contains("user_max_concurrent_positions"));
        assertFalse(validations.contains("requested_margin_usd > 20"));
        assertFalse(validations.contains("open_positions + reserved_positions > 5"));
        assertFalse(validations.contains("coalesce(a.open_positions, 0) + coalesce(p.pending_positions, 0) > 5"));
    }

    @Test
    void v3UserPositionLimitIsRequestScopedPersistedAndAtomicallyEnforced() throws IOException {
        String request = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyDispatchRequest.java");
        String coordinator = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyDispatchCoordinator.java");
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");
        String entity = source("src/main/java/com/apunto/engine/entity/CopyDispatchIntentEntity.java");
        String migration = source("src/main/resources/db/migration/V202607130005__copy_dispatch_user_position_limit_snapshot_v3.sql");

        assertTrue(request.contains("Integer userMaxConcurrentPositions"));
        assertTrue(coordinator.contains("allocation.getUserMaxConcurrentPositions()"));
        assertTrue(store.contains("new MicroLiveBudgetPolicy("));
        assertTrue(store.contains("maxTotalMarginUsd, request.userMaxConcurrentPositions())"));
        assertFalse(store.contains("copy.micro-live.user-max-concurrent-positions"));
        assertTrue(entity.contains("user_max_concurrent_positions"));
        assertTrue(migration.contains("copy_dispatch_intent_user_position_limit_chk"));
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
