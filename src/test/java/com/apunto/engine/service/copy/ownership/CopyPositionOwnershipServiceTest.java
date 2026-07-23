package com.apunto.engine.service.copy.ownership;

import com.apunto.engine.entity.CopyPositionOwnershipEntity;
import com.apunto.engine.repository.CopyPositionOwnershipRepository;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyPositionOwnershipServiceTest {

    @Test
    void oneAllocationOwnsAnAccountSymbolButAnotherAccountMayUseTheSameSymbol() {
        AtomicReference<CopyPositionOwnershipEntity> stored = new AtomicReference<>();
        CopyPositionOwnershipService service = new CopyPositionOwnershipService(repository(stored));
        UUID account = UUID.randomUUID();
        UUID cycle = UUID.randomUUID();

        CopyPositionOwnershipDecision first = service.claimOpen(
                10L, account, cycle, "BTCUSDC", "LONG", new BigDecimal("5"),
                "CROSSED", "ONE_WAY");
        assertTrue(first.allowed());

        CopyPositionOwnershipDecision competing = service.claimOpen(
                20L, account, UUID.randomUUID(), "btcusdc", "LONG", new BigDecimal("5"),
                "CROSSED", "ONE_WAY");
        assertFalse(competing.allowed());
        assertEquals("ACCOUNT_SYMBOL_ALREADY_OWNED", competing.reasonCode());

        stored.set(null);
        CopyPositionOwnershipDecision separateAccount = service.claimOpen(
                20L, UUID.randomUUID(), UUID.randomUUID(), "BTCUSDC", "LONG", new BigDecimal("20"),
                "ISOLATED", "ONE_WAY");
        assertTrue(separateAccount.allowed());
    }

    @Test
    void fillUpdatesVirtualOwnedQuantityAndCloseNeverGoesBelowZero() {
        AtomicReference<CopyPositionOwnershipEntity> stored = new AtomicReference<>();
        CopyPositionOwnershipService service = new CopyPositionOwnershipService(repository(stored));
        UUID account = UUID.randomUUID();
        service.claimOpen(10L, account, UUID.randomUUID(), "ETHUSDC", "SHORT",
                new BigDecimal("5"), "CROSSED", "ONE_WAY");

        service.recordFill(10L, account, "ETHUSDC", CopyOwnershipFillType.OPEN,
                new BigDecimal("0.20"), new BigDecimal("0.20"));
        assertEquals(0, new BigDecimal("0.20").compareTo(stored.get().getOwnedQty()));

        service.recordFill(10L, account, "ETHUSDC", CopyOwnershipFillType.CLOSE,
                new BigDecimal("0.30"), BigDecimal.ZERO);
        assertEquals(0, BigDecimal.ZERO.compareTo(stored.get().getOwnedQty()));
        assertEquals("CLOSED", stored.get().getOwnershipStatus());
    }

    @Test
    void partialCloseCannotReleaseOwnershipAsIfTheAccountWereFlat() {
        AtomicReference<CopyPositionOwnershipEntity> stored = new AtomicReference<>();
        CopyPositionOwnershipService service = new CopyPositionOwnershipService(repository(stored));
        UUID account = UUID.randomUUID();
        service.claimOpen(10L, account, UUID.randomUUID(), "BTCUSDC", "LONG",
                new BigDecimal("5"), "CROSSED", "ONE_WAY");
        service.recordFill(10L, account, "BTCUSDC", CopyOwnershipFillType.OPEN,
                new BigDecimal("0.20"), new BigDecimal("0.20"));

        service.recordFill(10L, account, "BTCUSDC", CopyOwnershipFillType.CLOSE,
                new BigDecimal("0.10"), BigDecimal.ZERO);

        assertEquals(0, new BigDecimal("0.10").compareTo(stored.get().getOwnedQty()));
        assertEquals("RECONCILING", stored.get().getOwnershipStatus());
        assertTrue(stored.get().isReconciliationRequired());
    }

    @Test
    void deriskAuthorizationRejectsAnotherAllocationAndCapsAtOwnedAndObservedQuantity() {
        AtomicReference<CopyPositionOwnershipEntity> stored = new AtomicReference<>();
        CopyPositionOwnershipService service = new CopyPositionOwnershipService(repository(stored));
        UUID account = UUID.randomUUID();
        service.claimOpen(10L, account, UUID.randomUUID(), "ETHUSDC", "SHORT",
                new BigDecimal("5"), "CROSSED", "ONE_WAY");
        service.recordFill(10L, account, "ETHUSDC", CopyOwnershipFillType.OPEN,
                new BigDecimal("0.20"), new BigDecimal("0.12"));

        CopyPositionOwnershipDecision other = service.authorizeDerisk(
                20L, account, "ETHUSDC", new BigDecimal("0.10"));
        assertFalse(other.allowed());
        assertEquals("ACCOUNT_SYMBOL_ALREADY_OWNED", other.reasonCode());

        CopyPositionOwnershipDecision capped = service.authorizeDerisk(
                10L, account, "ETHUSDC", new BigDecimal("0.20"));
        assertTrue(capped.allowed());
        assertEquals("DERISK_QUANTITY_CAPPED_TO_OWNERSHIP", capped.reasonCode());
        assertEquals(0, new BigDecimal("0.12").compareTo(capped.authorizedQuantity()));
    }

    private static CopyPositionOwnershipRepository repository(
            AtomicReference<CopyPositionOwnershipEntity> stored) {
        return (CopyPositionOwnershipRepository) Proxy.newProxyInstance(
                CopyPositionOwnershipRepository.class.getClassLoader(),
                new Class<?>[]{CopyPositionOwnershipRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "findActiveForUpdate" -> Optional.ofNullable(stored.get());
                    case "saveAndFlush", "save" -> {
                        CopyPositionOwnershipEntity value = (CopyPositionOwnershipEntity) args[0];
                        if (value.getId() == null) value.setId(UUID.randomUUID());
                        stored.set(value);
                        yield value;
                    }
                    case "toString" -> "ownership-repository-stub";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
