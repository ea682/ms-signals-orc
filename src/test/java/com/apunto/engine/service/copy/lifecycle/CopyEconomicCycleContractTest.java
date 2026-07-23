package com.apunto.engine.service.copy.lifecycle;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyEconomicCycleContractTest {

    @Test
    void reopenCreatesANewCopyOperationAndEconomicCycleIdentity() throws IOException {
        String engine = source("src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java");
        String service = source("src/main/java/com/apunto/engine/service/impl/CopyOperationServiceImpl.java");
        String migration = source("src/main/resources/db/migration/V202607130002__copy_economic_execution_v3.sql");

        assertTrue(engine.contains("persistnewcycleafterorder(reopened"));
        assertFalse(engine.contains("reopened.setidoperation(existingcopy.getidoperation())"));
        assertFalse(engine.contains("actualizando_la_misma_fila"));
        assertTrue(service.contains("operation.setidoperation(entity.getidoperation())"));
        assertTrue(migration.contains("create table if not exists futuros_operaciones.copy_economic_cycle"));
        assertTrue(migration.contains("unique (copy_operation_id)"));
        assertTrue(migration.contains("cycle_sequence"));
    }

    @Test
    void unknownEconomicFactsRemainNullWithExplicitAvailabilityState() throws IOException {
        String migration = source("src/main/resources/db/migration/V202607130002__copy_economic_execution_v3.sql");

        for (String field : new String[]{
                "individual_fills", "entry_fee", "exit_fee", "total_fees",
                "funding_paid", "funding_received", "net_funding",
                "gross_realized_pnl", "net_realized_pnl", "unrealized_pnl",
                "expected_price", "actual_price", "slippage_bps", "slippage_usd",
                "submitted_at", "accepted_at", "filled_at", "persisted_at",
                "source_to_submit_latency_ms", "submit_to_fill_latency_ms", "end_to_end_latency_ms"}) {
            assertTrue(migration.contains(field), "missing economic field " + field);
        }
        assertTrue(migration.contains("economic_data_status"));
        assertTrue(migration.contains("'known', 'pending_reconciliation', 'unavailable'"));
        assertFalse(migration.contains("fee_usd numeric(38, 12) not null default 0"));
        assertFalse(migration.contains("funding_paid numeric(38, 12) not null default 0"));
    }

    @Test
    void persistedOperationAndCycleKeepImmutableExecutionAccountAndTradingSettings() throws IOException {
        String operationService = source("src/main/java/com/apunto/engine/service/impl/CopyOperationServiceImpl.java");
        String cycleService = source("src/main/java/com/apunto/engine/service/impl/CopyEconomicCycleServiceImpl.java");
        String operationDto = source("src/main/java/com/apunto/engine/dto/CopyOperationDto.java");

        assertTrue(operationService.contains("entity.setexchangeaccountid(operation.getexchangeaccountid())"));
        assertTrue(operationService.contains("entity.setsourcepositioncycleid(operation.getsourcepositioncycleid())"));
        assertTrue(operationDto.contains("private string fixedmarginmode"));
        assertTrue(operationDto.contains("private string fixedpositionmode"));
        assertTrue(cycleService.contains(".fixedmarginmode(operation.getfixedmarginmode())"));
        assertTrue(cycleService.contains(".fixedpositionmode(operation.getfixedpositionmode())"));
    }

    private String source(String relative) throws IOException {
        Path path = Path.of(relative);
        assertTrue(Files.isRegularFile(path), "missing source " + path.toAbsolutePath());
        return Files.readString(path).toLowerCase();
    }
}
