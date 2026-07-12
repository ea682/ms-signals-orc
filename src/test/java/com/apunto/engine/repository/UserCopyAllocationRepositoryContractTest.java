package com.apunto.engine.repository;

import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.repository.Query;

import java.lang.reflect.Method;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserCopyAllocationRepositoryContractTest {

    @Test
    void microLivePromotionEligibilityDoesNotDependOnAllocationPercentage() throws Exception {
        String sql = query("findMicroLivePromotionCandidates", int.class).toLowerCase();

        assertTrue(sql.contains("execution_mode, 'live') = 'micro_live'"));
        assertFalse(sql.contains("allocation_pct"));
    }

    @Test
    void executableAndUserCountsAreModeAware() throws Exception {
        String executable = query(
                "countActiveExecutableAllocationsByMode", String.class, String.class).toLowerCase();
        String eligible = query("countEligibleExecutionUsersByMode", String.class).toLowerCase();

        assertTrue(executable.contains(":executionmode = 'micro_live'"));
        assertTrue(executable.contains(":executionmode = 'live' and coalesce(uca.allocation_pct, 0) > 0"));
        assertTrue(eligible.contains(":executionmode = 'micro_live'"));
        assertTrue(eligible.contains(":executionmode = 'live' and coalesce(uca.allocation_pct, 0) > 0"));
    }

    @Test
    void runtimeSnapshotIncludesNullPercentageMicroButExcludesNullPercentageLive() throws Exception {
        String sql = query("findAllActiveRuntimeAllocations").toLowerCase();

        assertTrue(sql.contains("execution_mode, 'live') = 'micro_live'"));
        assertTrue(sql.contains("or coalesce(uca.allocation_pct, 0) > 0"));
    }

    private static String query(String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = UserCopyAllocationRepository.class.getMethod(methodName, parameterTypes);
        Query query = method.getAnnotation(Query.class);
        if (query == null) throw new AssertionError("missing @Query on " + methodName);
        return query.value().replaceAll("\\s+", " ").trim();
    }
}
