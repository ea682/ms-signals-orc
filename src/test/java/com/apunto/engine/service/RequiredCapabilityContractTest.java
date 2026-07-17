package com.apunto.engine.service;

import com.apunto.engine.service.copy.simulation.CopySimulationJobStore;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;

class RequiredCapabilityContractTest {

    @Test
    void productionCapabilitiesMustNotHideUnsupportedDefaultImplementations() throws Exception {
        assertFalse(ProcesBinanceService.class
                .getMethod("getPositions", String.class, String.class, String.class)
                .isDefault());
        assertFalse(CopySimulationJobStore.class
                .getMethod("requestPause", UUID.class)
                .isDefault());
        assertFalse(CopySimulationJobStore.class
                .getMethod("resume", UUID.class)
                .isDefault());
    }
}
