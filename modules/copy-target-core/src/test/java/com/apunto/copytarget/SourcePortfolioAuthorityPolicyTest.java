package com.apunto.copytarget;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourcePortfolioAuthorityPolicyTest {

    @Test
    void onlyCompleteMatchingFullyResolvedPortfolioMayInferAbsenceClose() {
        assertTrue(SourcePortfolioAuthorityPolicy.canInferAbsenceClose(
                true, 42L, 42L, 0, 0));
        assertTrue(SourcePortfolioAuthorityPolicy.canInferAbsenceClose(
                true, 42L, 42L, 3, 3));

        assertFalse(SourcePortfolioAuthorityPolicy.canInferAbsenceClose(
                false, 42L, 42L, 0, 0));
        assertFalse(SourcePortfolioAuthorityPolicy.canInferAbsenceClose(
                true, 42L, 41L, 0, 0));
        assertFalse(SourcePortfolioAuthorityPolicy.canInferAbsenceClose(
                true, null, 42L, 0, 0));
        assertFalse(SourcePortfolioAuthorityPolicy.canInferAbsenceClose(
                true, 42L, 42L, 3, 2));
        assertFalse(SourcePortfolioAuthorityPolicy.canInferAbsenceClose(
                true, 42L, 42L, -1, 0));
    }
}
