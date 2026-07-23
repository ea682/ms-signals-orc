package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class B2bRealMoneyExecutionGuard {
    private final B2bRealMoneyGuardProperties properties;
    private final Environment environment;
    private final AtomicReference<PositionReservation> positionReservation = new AtomicReference<>();
    private final AtomicInteger orderCount = new AtomicInteger();
    private final Map<String, OrderFingerprint> admittedOrders = new ConcurrentHashMap<>();

    @PostConstruct
    void logEffectiveConfiguration() {
        log.info("event=copy.b2b_real_money_guard.config enabled={} profileB2b={} emergencyStop={} manualPositionsVerified={} testUserConfigured={} microAccountConfigured={} liveAccountConfigured={} accountsIsolated={} allowedSymbols={} clientOrderIdPrefixConfigured={} maxTotalMarginUsd={} maxNotionalUsd={} maxLeverage={} maxOrders={}",
                properties.isEnabled(),
                environment.acceptsProfiles(Profiles.of("b2b")),
                properties.isEmergencyStop(),
                properties.isManualPositionsVerified(),
                properties.getTestUserId() != null && !properties.getTestUserId().isBlank(),
                properties.getMicroLiveExecutionAccountId() != null,
                properties.getLiveExecutionAccountId() != null,
                properties.getMicroLiveExecutionAccountId() != null
                        && !properties.getMicroLiveExecutionAccountId().equals(properties.getLiveExecutionAccountId()),
                symbols().size(),
                properties.getClientOrderIdPrefix() != null && !properties.getClientOrderIdPrefix().isBlank(),
                properties.getMaxTotalMarginUsd(),
                properties.getMaxNotionalUsd(),
                properties.getMaxLeverage(),
                properties.getMaxOrders());
    }

    public Decision evaluate(OperationDto operation) {
        if (!properties.isEnabled()) return Decision.allow("B2B_REAL_MONEY_GUARD_INACTIVE");
        if (!environment.acceptsProfiles(Profiles.of("b2b"))) {
            return Decision.block("B2B_REAL_MONEY_PROFILE_REQUIRED");
        }
        if (operation == null) return Decision.block("B2B_OPERATION_MISSING");
        if (!same(properties.getTestUserId(), operation.getUserId())) {
            return Decision.block("B2B_TEST_USER_BLOCKED");
        }
        if (!symbols().contains(normalize(operation.getSymbol()))) {
            return Decision.block("B2B_SYMBOL_BLOCKED");
        }
        String accountPurpose = normalize(operation.getAccountPurpose());
        UUID expectedAccountId = expectedAccountId(accountPurpose);
        if (expectedAccountId == null || operation.getExchangeAccountId() == null
                || !expectedAccountId.equals(operation.getExchangeAccountId())) {
            return Decision.block("B2B_EXECUTION_ACCOUNT_PURPOSE_MISMATCH");
        }
        String clientId = operation.getClientOrderId();
        if (clientId == null || !clientId.startsWith(properties.getClientOrderIdPrefix())) {
            return Decision.block("B2B_CLIENT_ORDER_ID_PREFIX_REQUIRED");
        }
        OrderFingerprint fingerprint = OrderFingerprint.from(operation);
        OrderFingerprint admitted = admittedOrders.get(clientId);
        if (admitted != null) {
            return admitted.equals(fingerprint)
                    ? Decision.allow("B2B_IDEMPOTENT_ORDER_RETRY_ALLOWED")
                    : Decision.block("B2B_CLIENT_ORDER_ID_PAYLOAD_CONFLICT");
        }
        if (operation.isReduceOnly()) {
            PositionReservation reserved = positionReservation.get();
            if (reserved != null && !reserved.matches(operation)) {
                return Decision.block("B2B_DERISK_POSITION_MISMATCH");
            }
            if (!reserveOrderSlot(true)) return Decision.block("B2B_ORDER_LIMIT_REACHED");
            admittedOrders.put(clientId, fingerprint);
            return Decision.allow("B2B_DERISK_TEST_POSITION_ALLOWED");
        }
        if (properties.isEmergencyStop()) return Decision.block("B2B_EMERGENCY_STOP_ACTIVE");
        if (!properties.isManualPositionsVerified()) {
            return Decision.block("B2B_MANUAL_POSITIONS_NOT_VERIFIED");
        }
        if (!positiveAtMost(operation.getRequestedMarginUsd(), properties.getMaxTotalMarginUsd())) {
            return Decision.block("B2B_MARGIN_LIMIT_EXCEEDED");
        }
        if (!positiveAtMost(operation.getRequestedNotionalUsd(), properties.getMaxNotionalUsd())) {
            return Decision.block("B2B_NOTIONAL_LIMIT_EXCEEDED");
        }
        int leverage = operation.getLeverage() == null ? 0 : operation.getLeverage();
        if (leverage < 1 || leverage > properties.getMaxLeverage()) {
            return Decision.block("B2B_MAX_LEVERAGE_EXCEEDED");
        }
        boolean increase = "INCREASE".equals(normalize(operation.getCopyIntent()));
        PositionReservation previous = null;
        PositionReservation claimed;
        if (increase) {
            while (true) {
                PositionReservation reserved = positionReservation.get();
                if (reserved == null) return Decision.block("B2B_POSITION_NOT_RESERVED");
                if (!reserved.matches(operation)) {
                    return Decision.block(reserved.sameAccount(operation)
                            ? "B2B_SINGLE_POSITION_LIMIT_REACHED"
                            : "B2B_ANOTHER_ACCOUNT_HAS_OPEN_POSITION");
                }
                BigDecimal totalMargin = reserved.marginUsd().add(operation.getRequestedMarginUsd());
                if (totalMargin.compareTo(properties.getMaxTotalMarginUsd()) > 0) {
                    return Decision.block("B2B_GLOBAL_MARGIN_LIMIT_EXCEEDED");
                }
                BigDecimal totalNotional = reserved.notionalUsd().add(operation.getRequestedNotionalUsd());
                if (totalNotional.compareTo(properties.getMaxNotionalUsd()) > 0) {
                    return Decision.block("B2B_GLOBAL_NOTIONAL_LIMIT_EXCEEDED");
                }
                claimed = reserved.withExposure(totalMargin, totalNotional);
                if (positionReservation.compareAndSet(reserved, claimed)) {
                    previous = reserved;
                    break;
                }
            }
        } else {
            claimed = PositionReservation.from(operation);
            if (!positionReservation.compareAndSet(null, claimed)) {
                PositionReservation reserved = positionReservation.get();
                return Decision.block(reserved != null && !reserved.sameAccount(operation)
                        ? "B2B_ANOTHER_ACCOUNT_HAS_OPEN_POSITION"
                        : "B2B_SINGLE_POSITION_LIMIT_REACHED");
            }
        }
        if (!reserveOrderSlot(false)) {
            positionReservation.compareAndSet(claimed, previous);
            return Decision.block("B2B_ORDER_LIMIT_REACHED");
        }
        admittedOrders.put(clientId, fingerprint);
        return Decision.allow("B2B_REAL_MONEY_LIMITS_PASSED");
    }

    /**
     * Releases the one-position reservation only after the caller has queried
     * Binance and verified that the account+symbol is flat and order-free.
     */
    public boolean releaseAfterVerifiedFlat(OperationDto operation) {
        PositionReservation reserved = positionReservation.get();
        return reserved != null && reserved.matches(operation)
                && positionReservation.compareAndSet(reserved, null);
    }

    public boolean isEnabled() {
        return properties.isEnabled();
    }

    private UUID expectedAccountId(String purpose) {
        if ("MICRO_LIVE".equals(purpose)) return properties.getMicroLiveExecutionAccountId();
        if ("LIVE".equals(purpose)) return properties.getLiveExecutionAccountId();
        return null;
    }

    private boolean reserveOrderSlot(boolean derisk) {
        while (true) {
            int current = orderCount.get();
            int limit = properties.getMaxOrders();
            int ceiling = derisk ? limit : limit - 1; // retain one slot for a safe CLOSE
            if (current >= ceiling) return false;
            if (orderCount.compareAndSet(current, current + 1)) return true;
        }
    }

    private Set<String> symbols() {
        if (properties.getAllowedSymbols() == null) return Set.of();
        return Arrays.stream(properties.getAllowedSymbols().split(","))
                .map(B2bRealMoneyExecutionGuard::normalize)
                .filter(value -> value != null && !value.isBlank())
                .collect(Collectors.toSet());
    }

    private static boolean positiveAtMost(BigDecimal value, BigDecimal limit) {
        return value != null && value.signum() > 0 && limit != null && value.compareTo(limit) <= 0;
    }

    private static boolean same(String expected, String actual) {
        return normalize(expected) != null && normalize(expected).equals(normalize(actual));
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private record OrderFingerprint(
            String userId,
            String symbol,
            String marginUsd,
            String notionalUsd,
            Integer leverage,
            boolean reduceOnly,
            String copyIntent,
            String exchangeAccountId,
            String accountPurpose,
            String sourcePositionCycleId
    ) {
        static OrderFingerprint from(OperationDto operation) {
            return new OrderFingerprint(
                    normalize(operation.getUserId()),
                    normalize(operation.getSymbol()),
                    decimal(operation.getRequestedMarginUsd()),
                    decimal(operation.getRequestedNotionalUsd()),
                    operation.getLeverage(),
                    operation.isReduceOnly(),
                    normalize(operation.getCopyIntent()),
                    operation.getExchangeAccountId() == null ? null : operation.getExchangeAccountId().toString(),
                    normalize(operation.getAccountPurpose()),
                    operation.getSourcePositionCycleId() == null ? null : operation.getSourcePositionCycleId().toString()
            );
        }

        private static String decimal(BigDecimal value) {
            return value == null ? null : value.stripTrailingZeros().toPlainString();
        }
    }

    private record PositionReservation(
            String userId,
            String symbol,
            String exchangeAccountId,
            String accountPurpose,
            String sourcePositionCycleId,
            BigDecimal marginUsd,
            BigDecimal notionalUsd
    ) {
        static PositionReservation from(OperationDto operation) {
            return new PositionReservation(
                    normalize(operation.getUserId()),
                    normalize(operation.getSymbol()),
                    operation.getExchangeAccountId() == null ? null : operation.getExchangeAccountId().toString(),
                    normalize(operation.getAccountPurpose()),
                    operation.getSourcePositionCycleId() == null ? null : operation.getSourcePositionCycleId().toString(),
                    operation.getRequestedMarginUsd(),
                    operation.getRequestedNotionalUsd());
        }

        boolean matches(OperationDto operation) {
            PositionReservation candidate = from(operation);
            boolean sameCycle = candidate.sourcePositionCycleId == null || sourcePositionCycleId == null
                    || candidate.sourcePositionCycleId.equals(sourcePositionCycleId);
            return userId != null && userId.equals(candidate.userId)
                    && symbol != null && symbol.equals(candidate.symbol)
                    && java.util.Objects.equals(exchangeAccountId, candidate.exchangeAccountId)
                    && java.util.Objects.equals(accountPurpose, candidate.accountPurpose)
                    && sameCycle;
        }

        boolean sameAccount(OperationDto operation) {
            return java.util.Objects.equals(exchangeAccountId,
                    operation.getExchangeAccountId() == null ? null : operation.getExchangeAccountId().toString())
                    && java.util.Objects.equals(accountPurpose, normalize(operation.getAccountPurpose()));
        }

        PositionReservation withExposure(BigDecimal margin, BigDecimal notional) {
            return new PositionReservation(userId, symbol, exchangeAccountId, accountPurpose,
                    sourcePositionCycleId, margin, notional);
        }
    }

    public record Decision(boolean allowed, String reasonCode) {
        static Decision allow(String reason) { return new Decision(true, reason); }
        static Decision block(String reason) { return new Decision(false, reason); }
    }
}
