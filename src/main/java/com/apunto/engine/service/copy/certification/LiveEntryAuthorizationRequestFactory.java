package com.apunto.engine.service.copy.certification;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class LiveEntryAuthorizationRequestFactory {

    private final LiveCertificationRuntimeProperties properties;

    public LiveEntryAuthorizationContext create(OperationDto operation, UserCopyAllocationEntity allocation) {
        if (operation == null || allocation == null) {
            return LiveEntryAuthorizationContext.invalid("LIVE_CERTIFICATION_RUNTIME_CONTEXT_INCOMPLETE");
        }
        try {
            BigDecimal leverage = firstPositive(
                    operation.getTargetLeverage(),
                    operation.getLeverage() == null ? null : BigDecimal.valueOf(operation.getLeverage()),
                    allocation.getLeverageOverride());
            LiveEntryAuthorizationRequest request = new LiveEntryAuthorizationRequest(
                    UUID.fromString(required(operation.getUserId())),
                    allocation.getId(),
                    firstNonBlank(operation.getWalletId(), allocation.getWalletId()),
                    allocation.getCopyStrategyCode(),
                    firstNonBlank(operation.getStrategyVersion(), properties.getStrategyVersion()),
                    allocation.getScopeType(),
                    allocation.getScopeValue(),
                    operation.getTargetAllocatedCapitalUsd(),
                    leverage,
                    firstNonBlank(operation.getExchange(), properties.getExchange()),
                    firstNonBlank(operation.getQuoteAsset(), allocation.getResolvedQuoteAsset(), allocation.getCapitalAsset()),
                    firstNonBlank(operation.getSizingPolicyVersion(), properties.getSizingPolicyVersion()),
                    firstNonBlank(operation.getSymbolMappingVersion(), properties.getSymbolMappingVersion()),
                    firstNonBlank(operation.getFeeModelVersion(), properties.getFeeModelVersion()),
                    firstNonBlank(operation.getFundingModelVersion(), properties.getFundingModelVersion()),
                    firstNonBlank(operation.getSlippageModelVersion(), properties.getSlippageModelVersion()),
                    firstNonBlank(operation.getLiquidityModelVersion(), properties.getLiquidityModelVersion()));
            return LiveEntryAuthorizationContext.valid(request);
        } catch (IllegalArgumentException | NullPointerException ex) {
            return LiveEntryAuthorizationContext.invalid("LIVE_CERTIFICATION_RUNTIME_CONTEXT_INCOMPLETE");
        }
    }

    private static BigDecimal firstPositive(BigDecimal... values) {
        if (values == null) return null;
        for (BigDecimal value : values) {
            if (value != null && value.signum() > 0) return value;
        }
        return null;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) return value.trim();
        }
        return null;
    }

    private static String required(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("required value is missing");
        return value.trim();
    }
}
