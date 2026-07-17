package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.entity.CopyDispatchIntentEntity;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@Component
public class CopyDispatchPayloadSnapshotFactory {

    public CopyDispatchPayloadComparison compare(CopyDispatchIntentEntity existing,
                                                 CopyDispatchRequest incoming) {
        Map<String, Object> prior = existing(existing);
        Map<String, Object> next = incoming(incoming);
        Map<String, Map<String, Object>> diff = new LinkedHashMap<>();
        prior.forEach((field, priorValue) -> {
            Object nextValue = next.get(field);
            if (!Objects.equals(priorValue, nextValue)) {
                Map<String, Object> change = new LinkedHashMap<>();
                change.put("existing", priorValue);
                change.put("incoming", nextValue);
                diff.put(field, immutableCopy(change));
            }
        });
        return new CopyDispatchPayloadComparison(
                immutableCopy(prior), immutableCopy(next), immutableCopy(diff));
    }

    private Map<String, Object> existing(CopyDispatchIntentEntity value) {
        Map<String, Object> payload = new LinkedHashMap<>();
        put(payload, "userId", value.getIdUser());
        put(payload, "allocationId", value.getUserCopyAllocationId());
        put(payload, "executionMode", value.getExecutionMode());
        put(payload, "walletId", value.getWalletId());
        put(payload, "strategyCode", value.getStrategyCode());
        put(payload, "scopeType", value.getScopeType());
        put(payload, "scopeValue", value.getScopeValue());
        put(payload, "generationId", value.getMetricGenerationId());
        put(payload, "sourceEventId", value.getSourceEventId());
        put(payload, "sourceEventType", value.getSourceEventType());
        put(payload, "copyIntent", value.getCopyIntent());
        put(payload, "originId", value.getIdOrderOrigin());
        put(payload, "symbol", value.getSymbol());
        put(payload, "side", value.getSide());
        put(payload, "positionSide", value.getPositionSide());
        put(payload, "reduceOnly", value.isReduceOnly());
        put(payload, "requestedQty", decimal(value.getRequestedQty()));
        put(payload, "requestedMarginUsd", decimal(value.getRequestedMarginUsd()));
        put(payload, "requestedNotionalUsd", decimal(value.getRequestedNotionalUsd()));
        put(payload, "referencePrice", decimal(value.getReferencePrice()));
        put(payload, "requestedLeverage", value.getRequestedLeverage());
        put(payload, "userMaxConcurrentPositions", value.getUserMaxConcurrentPositions());
        put(payload, "reservePosition", value.getReservedPositionCount() > 0);
        put(payload, "clientOrderId", value.getClientOrderId());
        return payload;
    }

    private Map<String, Object> incoming(CopyDispatchRequest value) {
        CopyDispatchIdentity identity = value.identity();
        Map<String, Object> payload = new LinkedHashMap<>();
        put(payload, "userId", identity.userId());
        put(payload, "allocationId", identity.userCopyAllocationId());
        put(payload, "executionMode", identity.executionMode());
        put(payload, "walletId", value.walletId());
        put(payload, "strategyCode", identity.strategyCode());
        put(payload, "scopeType", identity.scopeType());
        put(payload, "scopeValue", identity.scopeValue());
        put(payload, "generationId", identity.generationId());
        put(payload, "sourceEventId", identity.sourceEventId());
        put(payload, "sourceEventType", value.sourceEventType());
        put(payload, "copyIntent", identity.copyIntent());
        put(payload, "originId", value.operation() == null ? null : value.operation().getOriginId());
        put(payload, "symbol", value.symbol());
        put(payload, "side", value.side());
        put(payload, "positionSide", value.positionSide());
        put(payload, "reduceOnly", value.reduceOnly());
        put(payload, "requestedQty", decimal(value.requestedQty()));
        put(payload, "requestedMarginUsd", decimal(value.requestedMarginUsd()));
        put(payload, "requestedNotionalUsd", decimal(value.requestedNotionalUsd()));
        put(payload, "referencePrice", decimal(value.referencePrice()));
        put(payload, "requestedLeverage", value.requestedLeverage());
        put(payload, "userMaxConcurrentPositions", value.userMaxConcurrentPositions());
        put(payload, "reservePosition", value.reservePosition());
        put(payload, "clientOrderId", value.operation() == null ? null : value.operation().getClientOrderId());
        return payload;
    }

    private static String decimal(BigDecimal value) {
        return value == null ? null : value.stripTrailingZeros().toPlainString();
    }

    private static void put(Map<String, Object> target, String key, Object value) {
        target.put(key, value);
    }

    private static <K, V> Map<K, V> immutableCopy(Map<K, V> source) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }
}
