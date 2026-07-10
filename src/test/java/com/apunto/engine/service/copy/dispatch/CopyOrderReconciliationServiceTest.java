package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.repository.CopyDispatchIntentRepository;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.UUID;

class CopyOrderReconciliationServiceTest {

    @Test
    void firstClaimLeaseIsAtLeastDispatchStaleWindow() {
        assertEquals(30L, CopyOrderReconciliationService.claimLeaseSeconds(Duration.ofSeconds(30), 1));
    }

    @Test
    void exponentialBackoffCanExtendClaimLease() {
        assertEquals(300L, CopyOrderReconciliationService.claimLeaseSeconds(Duration.ofSeconds(30), 10));
    }

    @Test
    void missingLeaseConfigurationStillHasSafeDefault() {
        assertEquals(30L, CopyOrderReconciliationService.claimLeaseSeconds(null, 1));
    }

    @Test
    void exhaustedAmbiguousLookupMovesToManualReviewAndKeepsReservation() {
        CopyDispatchIntentEntity intent = intent(20);
        CopyOrderReconciliationService service = new CopyOrderReconciliationService(repository(intent));

        service.markLookupNotFound(intent.getId(), 20);

        assertEquals("MANUAL_REVIEW", intent.getStatus());
        assertEquals("PENDING", intent.getReservationStatus());
        assertNull(intent.getNextReconciliationAt());
    }

    @Test
    void exhaustedPriceResolutionMovesToManualReviewButKeepsConfirmedEffect() {
        CopyDispatchIntentEntity intent = intent(20);
        CopyOrderReconciliationService service = new CopyOrderReconciliationService(repository(intent));

        service.markPriceResolutionExhausted(intent.getId());

        assertEquals("MANUAL_REVIEW", intent.getStatus());
        assertEquals("CONFIRMED", intent.getReservationStatus());
    }

    private CopyDispatchIntentEntity intent(int attempts) {
        return CopyDispatchIntentEntity.builder().id(UUID.randomUUID()).status("RECONCILING")
                .reservationStatus("PENDING").reconciliationAttempts(attempts).build();
    }

    private CopyDispatchIntentRepository repository(CopyDispatchIntentEntity intent) {
        return (CopyDispatchIntentRepository) Proxy.newProxyInstance(
                CopyDispatchIntentRepository.class.getClassLoader(),
                new Class<?>[]{CopyDispatchIntentRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "findById" -> Optional.of(intent);
                    case "saveAndFlush", "save" -> args[0];
                    case "flush" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
