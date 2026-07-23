package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class AutomaticLiveCertificationService {

    private static final BigDecimal MAX_CERTIFIED_CAPITAL_USD = new BigDecimal("1000000000");
    private static final BigDecimal MICRO_LIVE_LEVERAGE = new BigDecimal("5");
    private static final String ACTOR = "automatic-micro-live-certification";

    private final LiveCertificationCatalogStore catalogStore;
    private final ManualLiveCertificationCatalogService catalogService;
    private final ManualLiveCertificationService transitionService;
    private final LiveCertificationRuntimeProperties properties;

    @Transactional
    public AutomaticLiveCertificationResult certify(UserCopyAllocationEntity allocation,
                                                      Map<String, Object> rawEvidence) {
        String validation = validate(allocation, rawEvidence);
        if (validation != null) return AutomaticLiveCertificationResult.blocked(validation, null);

        LiveCertificationIdentity identity = identity(allocation);
        Map<String, Object> evidence = evidence(allocation, rawEvidence);
        LiveCertificationCatalogRecord record = catalogStore.findByIdentity(identity).orElse(null);
        boolean created = false;
        if (record == null) {
            LiveCertificationCreateResult result = catalogService.create(new LiveCertificationCreateCommand(
                    identity,
                    LiveEvidenceLevel.MICRO_LIVE_CALIBRATED,
                    LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                    ACTOR,
                    "real Micro-live execution policy passed",
                    evidence,
                    key("create", identity.toString())
            ));
            if (!result.created()) {
                record = catalogStore.findByIdentity(identity).orElse(null);
                if (record == null) {
                    return AutomaticLiveCertificationResult.blocked(result.reasonCode(), null);
                }
            } else {
                created = !result.idempotent();
                record = catalogStore.findByIdentity(identity).orElse(null);
                if (record == null) {
                    return AutomaticLiveCertificationResult.blocked(
                            "LIVE_CERTIFICATION_NOT_VISIBLE_AFTER_CREATE", result.certification().id());
                }
            }
        }

        if (record.status() == LiveCertificationStatus.LIVE_APPROVED) {
            return AutomaticLiveCertificationResult.approved(record.id(), true);
        }
        if (record.status() == LiveCertificationStatus.REVOKED) {
            return AutomaticLiveCertificationResult.blocked("LIVE_CERTIFICATION_REVOKED_TERMINAL", record.id());
        }
        if (record.status() != LiveCertificationStatus.MICRO_LIVE_VALIDATING
                && record.status() != LiveCertificationStatus.LIVE_DEGRADED
                && record.status() != LiveCertificationStatus.SUSPENDED) {
            return AutomaticLiveCertificationResult.blocked(
                    "LIVE_CERTIFICATION_NOT_READY_FOR_AUTOMATIC_APPROVAL", record.id());
        }

        LiveCertificationTransitionResult transition = transitionService.transition(
                new LiveCertificationTransitionCommand(
                        record.id(),
                        record.version(),
                        record.status(),
                        LiveCertificationStatus.LIVE_APPROVED,
                        true,
                        ACTOR,
                        created ? "first valid real Micro-live test" : "valid real Micro-live recertification",
                        evidence,
                        key("approve", record.id() + ":" + allocation.getId() + ":" + evidence.get("submittedOrders"))
                )
        );
        return transition.applied()
                ? AutomaticLiveCertificationResult.approved(record.id(), transition.idempotent())
                : AutomaticLiveCertificationResult.blocked(transition.reasonCode(), record.id());
    }

    private LiveCertificationIdentity identity(UserCopyAllocationEntity allocation) {
        String quote = firstNonBlank(allocation.getResolvedQuoteAsset(), allocation.getCapitalAsset(), "USDC")
                .toUpperCase(Locale.ROOT);
        return new LiveCertificationIdentity(
                allocation.getWalletId(),
                allocation.getCopyStrategyCode(),
                properties.getStrategyVersion(),
                allocation.getScopeType(),
                allocation.getScopeValue(),
                BigDecimal.ZERO,
                MAX_CERTIFIED_CAPITAL_USD,
                MICRO_LIVE_LEVERAGE,
                properties.getExchange(),
                quote,
                properties.getSizingPolicyVersion(),
                properties.getSymbolMappingVersion(),
                properties.getFeeModelVersion(),
                properties.getFundingModelVersion(),
                properties.getSlippageModelVersion(),
                properties.getLiquidityModelVersion()
        );
    }

    private static Map<String, Object> evidence(UserCopyAllocationEntity allocation,
                                                 Map<String, Object> rawEvidence) {
        Map<String, Object> evidence = new LinkedHashMap<>(rawEvidence);
        evidence.put("automaticPolicyPassed", true);
        evidence.put("realMicroLiveEvidence", true);
        evidence.put("validMicroLiveTests", 1);
        evidence.put("microLiveAllocationId", allocation.getId());
        evidence.put("certifierUserId", allocation.getIdUser().toString());
        evidence.put("evidenceExecutionMode", "MICRO_LIVE");
        return Map.copyOf(evidence);
    }

    private static String validate(UserCopyAllocationEntity allocation, Map<String, Object> evidence) {
        if (allocation == null || allocation.getId() == null || allocation.getIdUser() == null
                || allocation.getWalletId() == null || allocation.getWalletId().isBlank()
                || !"MICRO_LIVE".equals(UserCopyAllocationEntity.normalizeExecutionMode(allocation.getExecutionMode()))) {
            return "LIVE_CERTIFICATION_MICRO_ALLOCATION_REQUIRED";
        }
        if (evidence == null || !positive(evidence.get("submittedOrders"))
                || !positive(evidence.get("filledOrders"))
                || !positive(evidence.get("closedOperations"))) {
            return "LIVE_CERTIFICATION_REAL_MICRO_EVIDENCE_REQUIRED";
        }
        return null;
    }

    private static boolean positive(Object value) {
        return value instanceof Number number && number.longValue() > 0L;
    }

    private static String key(String type, String input) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest((type + ":" + input).getBytes(StandardCharsets.UTF_8));
            return type + ":" + java.util.HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) return value.trim();
        }
        return "USDC";
    }
}

