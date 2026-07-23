package com.apunto.engine.service.copy.filter;

import com.apunto.engine.dto.client.BinanceFuturesSymbolFilterDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Locale;

@Component
public class BinanceOrderFilterPolicy {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    public BinanceMarketOrderRules resolveMarketRules(BinanceFuturesSymbolInfoClientDto symbol) {
        if (symbol == null || symbol.getFilters() == null) return null;
        BinanceFuturesSymbolFilterDto quantity = first(symbol.getFilters(), "MARKET_LOT_SIZE");
        String source = "MARKET_LOT_SIZE";
        if (quantity == null) {
            quantity = first(symbol.getFilters(), "LOT_SIZE");
            source = "LOT_SIZE";
        }
        if (quantity == null) return null;

        BigDecimal step = decimal(quantity.getStepSize());
        BigDecimal minimum = decimal(quantity.getMinQty());
        BigDecimal maximum = decimal(quantity.getMaxQty());
        if (!positive(step) || !positive(minimum) || !positive(maximum)) return null;

        BinanceFuturesSymbolFilterDto notional = first(symbol.getFilters(), "NOTIONAL");
        BinanceFuturesSymbolFilterDto legacyNotional = first(symbol.getFilters(), "MIN_NOTIONAL");
        BigDecimal minNotional = firstDecimal(
                notional == null ? null : notional.getMinNotional(),
                notional == null ? null : notional.getNotional(),
                legacyNotional == null ? null : legacyNotional.getMinNotional(),
                legacyNotional == null ? null : legacyNotional.getNotional());
        BigDecimal maxNotional = firstDecimal(notional == null ? null : notional.getMaxNotional());
        int precision = symbol.getQuantityPrecision() == null
                ? Math.max(0, step.stripTrailingZeros().scale())
                : Math.max(0, symbol.getQuantityPrecision());
        boolean marketSupported = symbol.getOrderTypes() == null
                || symbol.getOrderTypes().stream().anyMatch("MARKET"::equalsIgnoreCase);
        return new BinanceMarketOrderRules(
                code(symbol.getSymbol()), "TRADING".equalsIgnoreCase(symbol.getStatus()),
                marketSupported, code(symbol.getQuoteAsset()), code(symbol.getMarginAsset()),
                source, step, minimum, maximum, precision, minNotional, maxNotional);
    }

    public BinanceOrderFilterDecision evaluate(BinanceOrderIntent intent,
                                               BinanceFuturesSymbolInfoClientDto symbol,
                                               BigDecimal requestedQty,
                                               BigDecimal ownedQty,
                                               BigDecimal actualBinanceQty,
                                               BigDecimal referencePrice) {
        BinanceMarketOrderRules rules = resolveMarketRules(symbol);
        BigDecimal requested = nonNegative(requestedQty);
        if (rules == null) {
            return blocked(intent, "BINANCE_SYMBOL_RULES_UNAVAILABLE", requested, ZERO, ZERO,
                    ZERO, null, !intent.increasesExposure());
        }
        if (!rules.trading()) {
            return blocked(intent, "BINANCE_SYMBOL_NOT_TRADING", requested, ZERO, ZERO,
                    ZERO, rules, !intent.increasesExposure());
        }
        if (!rules.marketOrderSupported()) {
            return blocked(intent, "BINANCE_MARKET_ORDER_UNSUPPORTED", requested, ZERO, ZERO,
                    ZERO, rules, !intent.increasesExposure());
        }
        if (intent.increasesExposure() && !positive(rules.minNotional())) {
            return blocked(intent, "BINANCE_SYMBOL_RULES_UNAVAILABLE", requested, ZERO, ZERO,
                    ZERO, rules, false);
        }
        BigDecimal capped = cap(intent, requested, ownedQty, actualBinanceQty);
        if (capped.signum() <= 0) {
            String reason = intent == BinanceOrderIntent.CLOSE || intent == BinanceOrderIntent.FLIP_CLOSE
                    ? "CLOSE_ALREADY_FLAT" : intent == BinanceOrderIntent.REDUCE
                    ? "REDUCE_NO_OWNED_QTY" : prefix(intent) + "_QTY_ZERO_NOOP";
            return blocked(intent, reason, requested, capped, ZERO, ZERO, rules, false);
        }

        BigDecimal rounded = roundDown(capped, rules.stepSize(), rules.quantityPrecision());
        BigDecimal price = nonNegative(referencePrice);
        BigDecimal notional = rounded.multiply(price);
        if (rounded.signum() <= 0 || rounded.compareTo(rules.minQty()) < 0) {
            boolean dust = intent == BinanceOrderIntent.CLOSE || intent == BinanceOrderIntent.FLIP_CLOSE;
            String reason = dust ? "CLOSE_DUST_RECONCILIATION_REQUIRED"
                    : intent == BinanceOrderIntent.INCREASE ? "INCREASE_BELOW_MIN_QTY_NOOP"
                    : intent.increasesExposure() ? "BINANCE_QTY_BELOW_MIN_QTY"
                    : "REDUCE_BELOW_EXCHANGE_MIN_NOOP";
            return blocked(intent, reason,
                    requested, capped, rounded, notional, rules, dust);
        }
        if (rounded.compareTo(rules.maxQty()) > 0) {
            return blocked(intent, intent.increasesExposure() ? "BINANCE_QTY_ABOVE_MAX_QTY"
                            : prefix(intent) + "_ABOVE_MAX_QTY", requested, capped,
                    rounded, notional, rules, !intent.increasesExposure());
        }
        if (intent.increasesExposure() && positive(rules.minNotional())
                && notional.compareTo(rules.minNotional()) < 0) {
            boolean dust = intent == BinanceOrderIntent.CLOSE || intent == BinanceOrderIntent.FLIP_CLOSE;
            String reason = dust ? "CLOSE_DUST_RECONCILIATION_REQUIRED"
                    : intent == BinanceOrderIntent.INCREASE ? "INCREASE_BELOW_MIN_NOTIONAL_NOOP"
                    : "BINANCE_NOTIONAL_BELOW_MIN";
            return blocked(intent, reason,
                    requested, capped, rounded, notional, rules, dust);
        }
        if (positive(rules.maxNotional()) && notional.compareTo(rules.maxNotional()) > 0) {
            return blocked(intent, intent.increasesExposure() ? "BINANCE_NOTIONAL_ABOVE_MAX"
                            : prefix(intent) + "_ABOVE_MAX_NOTIONAL", requested, capped,
                    rounded, notional, rules, !intent.increasesExposure());
        }
        return new BinanceOrderFilterDecision(true, false, prefix(intent) + "_FILTERS_OK",
                requested, capped, rounded, notional, rules);
    }

    private static BigDecimal cap(BinanceOrderIntent intent, BigDecimal requested,
                                  BigDecimal owned, BigDecimal actual) {
        if (intent.increasesExposure()) return requested;
        BigDecimal safeOwned = nonNegative(owned);
        BigDecimal safeActual = nonNegative(actual);
        if (intent == BinanceOrderIntent.REDUCE) return requested.min(safeOwned).min(safeActual);
        return safeOwned.min(safeActual);
    }

    private static BigDecimal roundDown(BigDecimal quantity, BigDecimal step, int precision) {
        BigDecimal stepped = quantity.divide(step, 0, RoundingMode.DOWN).multiply(step);
        return stepped.setScale(Math.max(0, precision), RoundingMode.DOWN);
    }

    private static BinanceOrderFilterDecision blocked(BinanceOrderIntent intent, String reason,
                                                       BigDecimal requested, BigDecimal capped,
                                                       BigDecimal rounded, BigDecimal notional,
                                                       BinanceMarketOrderRules rules,
                                                       boolean reconciliation) {
        return new BinanceOrderFilterDecision(false, reconciliation, reason, requested, capped,
                rounded, notional, rules);
    }

    private static BinanceFuturesSymbolFilterDto first(List<BinanceFuturesSymbolFilterDto> filters,
                                                       String type) {
        return filters.stream().filter(value -> value != null && type.equalsIgnoreCase(value.getFilterType()))
                .findFirst().orElse(null);
    }

    private static BigDecimal firstDecimal(String... values) {
        for (String value : values) {
            BigDecimal parsed = decimal(value);
            if (parsed != null) return parsed;
        }
        return null;
    }

    private static BigDecimal decimal(String value) {
        try {
            return value == null || value.isBlank() ? null : new BigDecimal(value.trim());
        } catch (NumberFormatException error) {
            return null;
        }
    }

    private static boolean positive(BigDecimal value) {
        return value != null && value.signum() > 0;
    }

    private static BigDecimal nonNegative(BigDecimal value) {
        return value == null || value.signum() < 0 ? ZERO : value;
    }

    private static String prefix(BinanceOrderIntent intent) {
        return intent.name().toUpperCase(Locale.ROOT);
    }

    private static String code(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }
}
