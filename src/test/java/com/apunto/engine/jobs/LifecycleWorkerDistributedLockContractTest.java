package com.apunto.engine.jobs;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LifecycleWorkerDistributedLockContractTest {

    @Test
    void lifecycleWorkersUseDistinctBoundedDistributedSchedulerLocks() throws Exception {
        assertLocked(ShadowPromotionJobWorker.class, "promoteShadowToMicroLive",
                "copy-shadow-promotion");
        assertLocked(MicroLivePromotionJobWorker.class, "promoteMicroLiveToLive",
                "copy-micro-live-promotion");
        assertLocked(LiveAdoptionReconciliationWorker.class, "reconcile",
                "copy-live-adoption-reconciliation");
        assertLocked(MicroLiveRecertificationJobWorker.class, "processPendingCapacity",
                "copy-micro-live-recertification");
    }

    @Test
    void flywayCreatesThePostgresSchedulerLockTable() throws Exception {
        try (var stream = getClass().getResourceAsStream(
                "/db/migration/V202607180004__copy_lifecycle_scheduler_locks.sql")) {
            assertNotNull(stream, "the distributed scheduler-lock migration must exist");
            String sql = new String(stream.readAllBytes(), StandardCharsets.UTF_8).toLowerCase();
            assertTrue(sql.contains("create table") && sql.contains("futuros_operaciones.shedlock"));
            assertTrue(sql.contains("lock_until") && sql.contains("locked_at") && sql.contains("locked_by"));
        }
    }

    private static void assertLocked(Class<?> worker, String methodName, String expectedName)
            throws Exception {
        Method method = worker.getDeclaredMethod(methodName);
        SchedulerLock lock = method.getAnnotation(SchedulerLock.class);
        assertNotNull(lock, worker.getSimpleName() + " must use a distributed scheduler lock");
        assertTrue(expectedName.equals(lock.name()));
        assertFalse(lock.lockAtMostFor().isBlank(), "lockAtMostFor must bound crash recovery");
    }
}
