package com.apunto.engine.service.copy.quality;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@Component
public class RoundTripExecutionQualityCalculator {

    private static final BigDecimal TEN_THOUSAND = new BigDecimal("10000");

    public Result calculate(Request request) {
        String invalid = validate(request);
        if (invalid != null) return Result.incomplete(invalid, request);

        WeightedPrice originOpen = weighted(request.originOpenFills());
        WeightedPrice originClose = weighted(request.originCloseFills());
        WeightedPrice copyOpen = weighted(request.copyOpenFills());
        WeightedPrice copyClose = weighted(request.copyCloseFills());
        if (originOpen == null) return Result.incomplete("ORIGIN_OPEN_MISSING", request);
        if (originClose == null) return Result.incomplete("ORIGIN_CLOSE_MISSING", request);
        if (copyOpen == null) return Result.incomplete("COPY_OPEN_MISSING", request);
        if (copyClose == null) return Result.incomplete("COPY_CLOSE_MISSING", request);
        if (originOpen.quantity().compareTo(originClose.quantity()) != 0) {
            return Result.incomplete("ORIGIN_CYCLE_NOT_FULLY_CLOSED", request);
        }
        if (copyOpen.quantity().compareTo(copyClose.quantity()) != 0) {
            return Result.incomplete("COPY_CYCLE_NOT_FULLY_CLOSED", request);
        }

        BigDecimal originReturn = directionalReturn(request.side(), originOpen.price(), originClose.price());
        BigDecimal copyReturn = directionalReturn(request.side(), copyOpen.price(), copyClose.price());
        BigDecimal dragBps = originReturn.subtract(copyReturn)
                .multiply(TEN_THOUSAND).setScale(8, RoundingMode.HALF_UP);
        BigDecimal copyOpenNotional = copyOpen.price().multiply(copyOpen.quantity());
        BigDecimal fees = nonNegative(request.feesUsd());
        BigDecimal funding = nullToZero(request.fundingUsd());
        BigDecimal costBps = copyOpenNotional.signum() == 0
                ? BigDecimal.ZERO
                : fees.add(funding).divide(copyOpenNotional, 16, RoundingMode.HALF_UP)
                .multiply(TEN_THOUSAND);

        return new Result(
                Status.COMPLETE,
                null,
                originOpen.price(),
                originClose.price(),
                copyOpen.price(),
                copyClose.price(),
                originReturn,
                copyReturn,
                dragBps,
                fees,
                funding,
                request.latencyMs(),
                dragBps.add(costBps).setScale(8, RoundingMode.HALF_UP)
        );
    }

    private static String validate(Request request) {
        if (request == null) return "ROUND_TRIP_REQUEST_MISSING";
        if (request.side() == null) return "POSITION_SIDE_MISSING";
        return null;
    }

    private static WeightedPrice weighted(List<Fill> fills) {
        if (fills == null || fills.isEmpty()) return null;
        BigDecimal quantity = BigDecimal.ZERO;
        BigDecimal notional = BigDecimal.ZERO;
        for (Fill fill : fills) {
            if (fill == null || fill.price() == null || fill.quantity() == null
                    || fill.price().signum() <= 0 || fill.quantity().signum() <= 0) {
                return null;
            }
            quantity = quantity.add(fill.quantity());
            notional = notional.add(fill.price().multiply(fill.quantity()));
        }
        return new WeightedPrice(
                notional.divide(quantity, 16, RoundingMode.HALF_UP),
                quantity.stripTrailingZeros());
    }

    private static BigDecimal directionalReturn(Side side, BigDecimal open, BigDecimal close) {
        BigDecimal priceMove = side == Side.LONG ? close.subtract(open) : open.subtract(close);
        return priceMove.divide(open, 16, RoundingMode.HALF_UP);
    }

    private static BigDecimal nonNegative(BigDecimal value) {
        BigDecimal normalized = nullToZero(value);
        return normalized.signum() < 0 ? BigDecimal.ZERO : normalized;
    }

    private static BigDecimal nullToZero(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    public enum Side { LONG, SHORT }
    public enum Status { COMPLETE, INCOMPLETE }

    public record Fill(BigDecimal price, BigDecimal quantity) {}

    public record Request(
            Side side,
            List<Fill> originOpenFills,
            List<Fill> originCloseFills,
            List<Fill> copyOpenFills,
            List<Fill> copyCloseFills,
            BigDecimal feesUsd,
            BigDecimal fundingUsd,
            Long latencyMs
    ) {}

    public record Result(
            Status status,
            String incompleteReason,
            BigDecimal originOpenPrice,
            BigDecimal originClosePrice,
            BigDecimal copyOpenPrice,
            BigDecimal copyClosePrice,
            BigDecimal originReturn,
            BigDecimal copyReturn,
            BigDecimal executionDragBps,
            BigDecimal feesUsd,
            BigDecimal fundingUsd,
            Long latencyMs,
            BigDecimal netTrackingErrorBps
    ) {
        static Result incomplete(String reason, Request request) {
            return new Result(Status.INCOMPLETE, reason, null, null, null, null,
                    null, null, null,
                    request == null ? BigDecimal.ZERO : nonNegative(request.feesUsd()),
                    request == null ? BigDecimal.ZERO : nullToZero(request.fundingUsd()),
                    request == null ? null : request.latencyMs(), null);
        }

        public boolean complete() {
            return status == Status.COMPLETE;
        }
    }

    private record WeightedPrice(BigDecimal price, BigDecimal quantity) {}
}

