package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.shared.enums.OrderType;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Objects;

final class CopyDispatchRequestFingerprint {

    private static final String VERSION = "copy-order-payload-v2";

    private CopyDispatchRequestFingerprint() {
    }

    static String canonical(OperationDto operation,
                            BigDecimal requestedQty,
                            Integer userMaxConcurrentPositions,
                            boolean reservePosition) {
        Objects.requireNonNull(operation, "operation is required");
        return String.join("|",
                VERSION,
                text(operation.getExchangeAccountId() == null ? null : operation.getExchangeAccountId().toString()),
                upper(operation.getAccountPurpose()),
                text(operation.getSourcePositionCycleId() == null ? null : operation.getSourcePositionCycleId().toString()),
                upper(operation.getSymbol()),
                enumName(operation.getSide()),
                enumName(operation.getPositionSide()),
                enumName(operation.getType()),
                decimal(requestedQty),
                decimal(operation.getPrice()),
                upper(operation.getTimeInForce()),
                integer(operation.getLeverage()),
                integer(userMaxConcurrentPositions),
                Boolean.toString(reservePosition),
                Boolean.toString(operation.isReduceOnly()),
                Boolean.toString(Boolean.TRUE.equals(operation.getConfigureAccountSettings())),
                text(operation.getClientOrderId()));
    }

    static boolean matchesLegacyMarketWithDerivedEconomicsDrift(CopyDispatchIntentEntity existing,
                                                                 CopyDispatchRequest incoming,
                                                                 CopyIdempotencyKeyFactory keyFactory) {
        if (existing == null || incoming == null || incoming.operation() == null || keyFactory == null) {
            return false;
        }
        OperationDto operation = incoming.operation();
        if (operation.getType() != OrderType.MARKET
                || (operation.getPrice() != null && !operation.getPrice().isBlank())
                || existing.getReservedPositionCount() > 0 != incoming.reservePosition()) {
            return false;
        }
        String legacyPayload = String.join("|",
                text(incoming.symbol()),
                enumName(operation.getSide()),
                enumName(operation.getPositionSide()),
                enumName(operation.getType()),
                decimal(incoming.requestedQty()),
                decimal(existing.getRequestedMarginUsd()),
                decimal(existing.getRequestedNotionalUsd()),
                decimal(existing.getReferencePrice()),
                integer(operation.getLeverage()),
                integer(incoming.userMaxConcurrentPositions()),
                Boolean.toString(incoming.reduceOnly()),
                Boolean.toString(Boolean.TRUE.equals(operation.getConfigureAccountSettings())),
                text(operation.getClientOrderId()));
        return Objects.equals(existing.getRequestHash(), keyFactory.hashPayload(legacyPayload));
    }

    private static String decimal(String value) {
        if (value == null || value.isBlank()) return "";
        try {
            return decimal(new BigDecimal(value.trim()));
        } catch (NumberFormatException ignored) {
            return value.trim();
        }
    }

    private static String decimal(BigDecimal value) {
        return value == null ? "" : value.stripTrailingZeros().toPlainString();
    }

    private static String integer(Integer value) {
        return value == null ? "" : value.toString();
    }

    private static String enumName(Enum<?> value) {
        return value == null ? "" : value.name().toUpperCase(Locale.ROOT);
    }

    private static String upper(String value) {
        return text(value).toUpperCase(Locale.ROOT);
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
    }
}
