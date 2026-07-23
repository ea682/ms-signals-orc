package com.apunto.engine.service.copy.quality;

import com.apunto.engine.dto.client.BinanceExecutionFillClientDto;
import com.apunto.engine.entity.CopyOperationEventEntity;
import com.apunto.engine.repository.CopyOperationEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class RoundTripExecutionQualityPersistenceService {

    private final CopyOperationEventRepository eventRepository;
    private final RoundTripExecutionQualityCalculator calculator;
    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    @Transactional
    public void recalculate(Long allocationId, UUID economicCycleId) {
        if (allocationId == null || economicCycleId == null) return;
        List<CopyOperationEventEntity> events = eventRepository
                .findAllByUserCopyAllocationIdAndEconomicCycleIdOrderByEventTimeAscDateCreationAsc(
                        allocationId, economicCycleId);
        if (events.isEmpty()) return;

        RoundTripExecutionQualityCalculator.Side side = side(events);
        List<RoundTripExecutionQualityCalculator.Fill> originOpen = new ArrayList<>();
        List<RoundTripExecutionQualityCalculator.Fill> originClose = new ArrayList<>();
        List<RoundTripExecutionQualityCalculator.Fill> copyOpen = new ArrayList<>();
        List<RoundTripExecutionQualityCalculator.Fill> copyClose = new ArrayList<>();
        BigDecimal fees = BigDecimal.ZERO;
        BigDecimal funding = BigDecimal.ZERO;
        Long latency = null;
        for (CopyOperationEventEntity event : events) {
            boolean opening = opening(event);
            boolean closing = closing(event);
            if (!opening && !closing) continue;
            BigDecimal quantity = quantity(event);
            if (positive(event.getExpectedPrice()) && positive(quantity)) {
                (opening ? originOpen : originClose).add(
                        new RoundTripExecutionQualityCalculator.Fill(event.getExpectedPrice(), quantity));
            }
            List<RoundTripExecutionQualityCalculator.Fill> actual = copyFills(event, quantity);
            (opening ? copyOpen : copyClose).addAll(actual);
            if (event.getFeeUsd() != null && event.getFeeUsd().signum() > 0) fees = fees.add(event.getFeeUsd());
            if (event.getNetFunding() != null) funding = funding.add(event.getNetFunding());
            if (event.getEndToEndLatencyMs() != null) {
                latency = latency == null ? event.getEndToEndLatencyMs()
                        : Math.max(latency, event.getEndToEndLatencyMs());
            }
        }

        RoundTripExecutionQualityCalculator.Result result = calculator.calculate(
                new RoundTripExecutionQualityCalculator.Request(
                        side, originOpen, originClose, copyOpen, copyClose, fees, funding, latency));
        CopyOperationEventEntity last = events.getLast();
        jdbcTemplate.update("""
                        insert into futuros_operaciones.copy_round_trip_execution_quality (
                            id, allocation_id, economic_cycle_id, execution_mode, position_side,
                            status, incomplete_reason, origin_open_price, origin_close_price,
                            copy_open_price, copy_close_price, origin_return, copy_return,
                            execution_drag_bps, fees_usd, funding_usd, latency_ms,
                            net_tracking_error_bps, calculated_at
                        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                        on conflict (allocation_id, economic_cycle_id) do update set
                            execution_mode = excluded.execution_mode,
                            position_side = excluded.position_side,
                            status = excluded.status,
                            incomplete_reason = excluded.incomplete_reason,
                            origin_open_price = excluded.origin_open_price,
                            origin_close_price = excluded.origin_close_price,
                            copy_open_price = excluded.copy_open_price,
                            copy_close_price = excluded.copy_close_price,
                            origin_return = excluded.origin_return,
                            copy_return = excluded.copy_return,
                            execution_drag_bps = excluded.execution_drag_bps,
                            fees_usd = excluded.fees_usd,
                            funding_usd = excluded.funding_usd,
                            latency_ms = excluded.latency_ms,
                            net_tracking_error_bps = excluded.net_tracking_error_bps,
                            calculated_at = now()
                        """,
                UUID.randomUUID(), allocationId, economicCycleId,
                mode(last.getExecutionMode()), side == null ? "UNKNOWN" : side.name(),
                result.status().name(), result.incompleteReason(), result.originOpenPrice(),
                result.originClosePrice(), result.copyOpenPrice(), result.copyClosePrice(),
                result.originReturn(), result.copyReturn(), result.executionDragBps(),
                result.feesUsd(), result.fundingUsd(), result.latencyMs(), result.netTrackingErrorBps());
        meterRegistry.counter("copy_round_trip_execution_quality_total",
                "status", result.status().name().toLowerCase(Locale.ROOT),
                "execution_mode", mode(last.getExecutionMode()).toLowerCase(Locale.ROOT)).increment();
        log.info("event=copy.round_trip_execution_quality allocationId={} economicCycleId={} executionMode={} status={} incompleteReason={} executionDragBps={} netTrackingErrorBps={} hardPromotionGate=false",
                allocationId, economicCycleId, mode(last.getExecutionMode()), result.status(),
                result.incompleteReason(), result.executionDragBps(), result.netTrackingErrorBps());
    }

    private static List<RoundTripExecutionQualityCalculator.Fill> copyFills(
            CopyOperationEventEntity event, BigDecimal fallbackQuantity) {
        List<RoundTripExecutionQualityCalculator.Fill> fills = new ArrayList<>();
        if (event.getIndividualFills() != null) {
            for (BinanceExecutionFillClientDto fill : event.getIndividualFills()) {
                if (fill != null && positive(fill.getPrice()) && positive(fill.getQuantity())) {
                    fills.add(new RoundTripExecutionQualityCalculator.Fill(fill.getPrice(), fill.getQuantity()));
                }
            }
        }
        if (!fills.isEmpty()) return fills;
        BigDecimal price = firstPositive(event.getActualPrice(), event.getAverageFillPrice(), event.getPrice());
        if (positive(price) && positive(fallbackQuantity)) {
            fills.add(new RoundTripExecutionQualityCalculator.Fill(price, fallbackQuantity));
        }
        return fills;
    }

    private static RoundTripExecutionQualityCalculator.Side side(List<CopyOperationEventEntity> events) {
        for (CopyOperationEventEntity event : events) {
            String value = token(event.getPositionSide());
            if ("LONG".equals(value)) return RoundTripExecutionQualityCalculator.Side.LONG;
            if ("SHORT".equals(value)) return RoundTripExecutionQualityCalculator.Side.SHORT;
            value = token(event.getTypeOperation());
            if ("LONG".equals(value)) return RoundTripExecutionQualityCalculator.Side.LONG;
            if ("SHORT".equals(value)) return RoundTripExecutionQualityCalculator.Side.SHORT;
        }
        return null;
    }

    private static boolean opening(CopyOperationEventEntity event) {
        return containsAction(event, "OPEN") || containsAction(event, "INCREASE");
    }

    private static boolean closing(CopyOperationEventEntity event) {
        return containsAction(event, "CLOSE") || containsAction(event, "REDUCE");
    }

    private static boolean containsAction(CopyOperationEventEntity event, String expected) {
        return token(event.getCopyAction()).contains(expected)
                || token(event.getCopyIntent()).contains(expected)
                || token(event.getEventType()).contains(expected);
    }

    private static BigDecimal quantity(CopyOperationEventEntity event) {
        return firstPositive(event.getExecutedQuantity(), event.getQtyExecuted(), event.getRequestedQuantity());
    }

    private static BigDecimal firstPositive(BigDecimal... values) {
        for (BigDecimal value : values) if (positive(value)) return value;
        return null;
    }

    private static String token(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String mode(String value) {
        String mode = token(value);
        return "MICRO_LIVE".equals(mode) ? "MICRO_LIVE" : "LIVE";
    }

    private static boolean positive(BigDecimal value) {
        return value != null && value.signum() > 0;
    }
}
