package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveCertificationTransitionPolicyTest {

    private final LiveCertificationTransitionPolicy policy = new LiveCertificationTransitionPolicy();

    @Test
    void automaticLiveApprovalRequiresRealMicroLiveEvidenceThatPassedPolicy() {
        CertificationTransitionDecision automatic = policy.evaluate(
                LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                LiveCertificationStatus.LIVE_APPROVED,
                true,
                "promotion-job",
                "thresholds passed",
                Map.of(
                        "automaticPolicyPassed", true,
                        "realMicroLiveEvidence", true,
                        "validMicroLiveTests", 1));

        assertTrue(automatic.allowed());
    }

    @Test
    void administrativeLiveApprovalCannotBypassTechnicalEvidence() {
        CertificationTransitionDecision manual = policy.evaluate(
                LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                LiveCertificationStatus.LIVE_APPROVED,
                false,
                "operator@example.com",
                "reviewed calibration and reconciliation",
                Map.of("ticket", "CHG-1"));

        assertFalse(manual.allowed());
        assertEquals("LIVE_CERTIFICATION_REAL_MICRO_EVIDENCE_REQUIRED", manual.reasonCode());

        CertificationTransitionDecision evidencedManual = policy.evaluate(
                LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                LiveCertificationStatus.LIVE_APPROVED,
                false,
                "operator@example.com",
                "reviewed calibration and reconciliation",
                Map.of(
                        "automaticPolicyPassed", true,
                        "realMicroLiveEvidence", true,
                        "validMicroLiveTests", 1));

        assertTrue(evidencedManual.allowed());
    }

    @Test
    void automaticSafetyDegradationIsAllowedWithEvidence() {
        CertificationTransitionDecision degraded = policy.evaluate(
                LiveCertificationStatus.LIVE_APPROVED,
                LiveCertificationStatus.LIVE_DEGRADED,
                true,
                "certification-monitor",
                "dispatch error threshold exceeded",
                Map.of("dispatchErrorRate", "0.25"));

        assertTrue(degraded.allowed());
    }

    @Test
    void revokedCertificationIsTerminal() {
        CertificationTransitionDecision decision = policy.evaluate(
                LiveCertificationStatus.REVOKED,
                LiveCertificationStatus.LIVE_APPROVED,
                false,
                "operator@example.com",
                "try to restore",
                Map.of("ticket", "INC-1"));

        assertFalse(decision.allowed());
        assertEquals("LIVE_CERTIFICATION_REVOKED_TERMINAL", decision.reasonCode());
    }

    @Test
    void missingEvidenceFailsClosed() {
        CertificationTransitionDecision decision = policy.evaluate(
                LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                LiveCertificationStatus.LIVE_APPROVED,
                false,
                "operator@example.com",
                "reviewed",
                Map.of());

        assertFalse(decision.allowed());
        assertEquals("LIVE_CERTIFICATION_EVIDENCE_REQUIRED", decision.reasonCode());
    }
}
