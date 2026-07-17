package com.apunto.engine.service.metric;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MetricWalletReadModeResolverTest {

    @Test
    void bindsEverySupportedModeAndRejectsInvalidValues() {
        assertEquals(MetricWalletReadMode.V1, resolver("V1").configuredMode());
        assertEquals(MetricWalletReadMode.COMPARE, resolver("compare").configuredMode());
        assertEquals(MetricWalletReadMode.V2, resolver("v2").configuredMode());
        assertThrows(IllegalStateException.class, () -> resolver("maybe").configuredMode());
    }

    private MetricWalletReadModeResolver resolver(String mode) {
        return new MetricWalletReadModeResolver(mode);
    }
}
