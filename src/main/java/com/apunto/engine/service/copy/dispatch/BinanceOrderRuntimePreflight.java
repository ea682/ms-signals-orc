package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.service.copy.filter.BinanceOrderFilterDecision;
import com.apunto.engine.service.copy.filter.BinanceOrderFilterPolicy;
import com.apunto.engine.service.copy.filter.BinanceOrderIntent;
import com.apunto.engine.shared.enums.OrderType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class BinanceOrderRuntimePreflight implements CopyOrderRuntimePreflight {

    private final ProcesBinanceService binanceGateway;
    private final BinanceOrderFilterPolicy filterPolicy;

    @Override
    public CopyOrderRuntimePreflightDecision evaluate(CopyDispatchRequest request) {
        if (request == null || request.operation() == null || request.identity() == null) {
            return CopyOrderRuntimePreflightDecision.blocked("BINANCE_ORDER_PREFLIGHT_CONTEXT_INVALID", false);
        }
        OperationDto operation = request.operation();
        String mode = code(request.identity().executionMode());
        if (operation.getExchangeAccountId() == null || !mode.equals(code(operation.getAccountPurpose()))) {
            return CopyOrderRuntimePreflightDecision.blocked("EXECUTION_ACCOUNT_PURPOSE_MISMATCH", false);
        }
        if (operation.getType() != OrderType.MARKET) {
            return CopyOrderRuntimePreflightDecision.blocked("BINANCE_MARKET_ORDER_UNSUPPORTED", false);
        }
        BinanceOrderIntent intent = intent(request.identity().copyIntent(), request.reduceOnly());
        if (intent == null) {
            return CopyOrderRuntimePreflightDecision.blocked("BINANCE_ORDER_INTENT_UNSUPPORTED", false);
        }
        if (intent.increasesExposure()) {
            BigDecimal leverage = operation.getTargetLeverage() != null
                    ? operation.getTargetLeverage()
                    : operation.getLeverage() == null ? null : BigDecimal.valueOf(operation.getLeverage());
            if (leverage == null || leverage.signum() <= 0) {
                return CopyOrderRuntimePreflightDecision.blocked("BINANCE_LEVERAGE_BRACKET_EXCEEDED", false);
            }
            if (blank(operation.getFixedMarginMode())) {
                return CopyOrderRuntimePreflightDecision.blocked("BINANCE_MARGIN_MODE_MISMATCH", false);
            }
            if (blank(operation.getFixedPositionMode())) {
                return CopyOrderRuntimePreflightDecision.blocked("BINANCE_POSITION_MODE_MISMATCH", false);
            }
        }

        BinanceFuturesSymbolInfoClientDto symbol = symbol(operation.getApiKey(), request.symbol());
        if (symbol != null && !blank(operation.getQuoteAsset())) {
            String expected = code(operation.getQuoteAsset());
            if (!expected.equals(code(symbol.getQuoteAsset())) || !expected.equals(code(symbol.getMarginAsset()))) {
                return CopyOrderRuntimePreflightDecision.blocked("BINANCE_MARGIN_ASSET_MISMATCH", false);
            }
        }
        BigDecimal owned = request.requestedQty();
        BigDecimal actual = request.requestedQty();
        BinanceOrderFilterDecision decision = filterPolicy.evaluate(
                intent, symbol, request.requestedQty(), owned, actual, request.referencePrice());
        if (!decision.sendOrder()) {
            return CopyOrderRuntimePreflightDecision.blocked(
                    decision.reasonCode(), decision.reconciliationRequired());
        }
        return CopyOrderRuntimePreflightDecision.allowed(decision.roundedQty(), decision.reasonCode());
    }

    private BinanceFuturesSymbolInfoClientDto symbol(String apiKey, String requestedSymbol) {
        try {
            List<BinanceFuturesSymbolInfoClientDto> symbols = binanceGateway.getSymbols(apiKey);
            if (symbols == null) return null;
            String expected = code(requestedSymbol);
            return symbols.stream().filter(value -> value != null && expected.equals(code(value.getSymbol())))
                    .findFirst().orElse(null);
        } catch (RuntimeException unavailable) {
            return null;
        }
    }

    private static BinanceOrderIntent intent(String value, boolean reduceOnly) {
        String normalized = code(value);
        if ("FLIP".equals(normalized)) normalized = reduceOnly ? "FLIP_CLOSE" : "FLIP_OPEN";
        if ("PANIC_CLOSE".equals(normalized) || "REDUCE_OR_CLOSE".equals(normalized)) {
            normalized = "CLOSE";
        }
        try {
            return normalized == null ? null : BinanceOrderIntent.valueOf(normalized);
        } catch (IllegalArgumentException unknown) {
            return null;
        }
    }

    private static String code(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
