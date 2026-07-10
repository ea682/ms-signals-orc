package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
