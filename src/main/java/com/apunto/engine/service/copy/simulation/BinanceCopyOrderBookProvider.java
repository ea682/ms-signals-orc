package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.OrderBookLevel;
import com.apunto.copytarget.OrderBookSnapshot;
import com.apunto.engine.dto.client.BinanceOrderBookLevelClientDto;
import com.apunto.engine.dto.client.BinanceOrderBookSnapshotClientDto;
import com.apunto.engine.service.ProcesBinanceService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class BinanceCopyOrderBookProvider implements CopyOrderBookProvider {

    private final ProcesBinanceService procesBinanceService;

    @Override
    public Optional<OrderBookSnapshot> snapshot(String symbol, int limit) {
        return procesBinanceService.getOrderBookSnapshot(symbol, limit).map(this::toCore);
    }

    private OrderBookSnapshot toCore(BinanceOrderBookSnapshotClientDto value) {
        return new OrderBookSnapshot(
                value.getSymbol(),
                value.getCapturedAt() == null ? Instant.now() : value.getCapturedAt(),
                value.getSource() == null ? "BINANCE_FAPI_DEPTH" : value.getSource(),
                value.getLastUpdateId(),
                levels(value.getBids()),
                levels(value.getAsks())
        );
    }

    private List<OrderBookLevel> levels(List<BinanceOrderBookLevelClientDto> values) {
        if (values == null) return List.of();
        return values.stream()
                .map(value -> new OrderBookLevel(value.getPrice(), value.getQuantity()))
                .toList();
    }
}
