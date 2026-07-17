package com.apunto.engine.service.metric;

import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
@Slf4j
public class PostgresMetricV2SnapshotPersistence implements MetricV2SnapshotPersistence {

    private static final String TABLE = "futuros_operaciones.metric_strategy_snapshot_v2";
    private static final String REPLACE_LOCK_SQL =
            "SELECT pg_advisory_xact_lock(7723326142966129682)";
    private static final String INSERT_SQL = """
            INSERT INTO futuros_operaciones.metric_strategy_snapshot_v2 (
                strategy_key, snapshot_type, generation_id, metric_version, source_version,
                wallet_id, strategy_code, scope_type, scope_value, computed_at, data_as_of,
                fetched_at, expires_at, decision_final, allow_new_entries, reason_codes, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)
            """;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper objectMapper;

    public PostgresMetricV2SnapshotPersistence(
            JdbcTemplate jdbc,
            TransactionTemplate transactions,
            ObjectMapper objectMapper
    ) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<MetricV2SnapshotStore.Snapshot> load() {
        List<PersistedRow> rows = jdbc.query("""
                SELECT snapshot_type, payload::text, fetched_at
                FROM futuros_operaciones.metric_strategy_snapshot_v2
                ORDER BY snapshot_type, strategy_key
                """, (rs, rowNum) -> new PersistedRow(
                SnapshotType.valueOf(rs.getString("snapshot_type")),
                readDto(rs.getString("payload")),
                rs.getTimestamp("fetched_at").toInstant()
        ));
        if (rows.isEmpty()) return Optional.empty();

        Map<String, MetricStrategySnapshotDto> summary = new LinkedHashMap<>();
        Map<String, MetricStrategySnapshotDto> full = new LinkedHashMap<>();
        Map<String, MetricStrategySnapshotDto> guard = new LinkedHashMap<>();
        Map<String, String> walletGenerations = new HashMap<>();
        Instant summaryFetchedAt = null;
        Instant fullFetchedAt = null;
        Instant guardFetchedAt = null;
        for (PersistedRow row : rows) {
            MetricStrategySnapshotDto dto = row.dto();
            Map<String, MetricStrategySnapshotDto> target = switch (row.type()) {
                case SUMMARY -> summary;
                case FULL -> full;
                case COPY_GUARD -> guard;
            };
            if (target.putIfAbsent(dto.getStrategyKey(), dto) != null) {
                throw new IllegalStateException("METRIC_V2_PERSISTED_DUPLICATE_KEY:" + dto.getStrategyKey());
            }
            switch (row.type()) {
                case SUMMARY -> {
                    summaryFetchedAt = latest(summaryFetchedAt, row.fetchedAt());
                    String previous = walletGenerations.putIfAbsent(dto.getWalletId(), dto.getGenerationId());
                    if (previous != null && !previous.equals(dto.getGenerationId())) {
                        throw new IllegalStateException("METRIC_V2_PERSISTED_MULTIPLE_GENERATIONS:" + dto.getWalletId());
                    }
                }
                case FULL -> fullFetchedAt = latest(fullFetchedAt, row.fetchedAt());
                case COPY_GUARD -> guardFetchedAt = latest(guardFetchedAt, row.fetchedAt());
            }
        }
        return Optional.of(new MetricV2SnapshotStore.Snapshot(
                Map.copyOf(summary),
                Map.copyOf(full),
                Map.copyOf(guard),
                Map.copyOf(walletGenerations),
                summaryFetchedAt,
                fullFetchedAt,
                guardFetchedAt
        ));
    }

    @Override
    public void replace(MetricV2SnapshotStore.Snapshot snapshot, Duration maxStaleness) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        Duration ttl = maxStaleness == null || maxStaleness.isNegative() || maxStaleness.isZero()
                ? Duration.ofMinutes(10)
                : maxStaleness;
        List<WriteRow> rows = flatten(snapshot);
        transactions.executeWithoutResult(status -> {
            jdbc.execute(REPLACE_LOCK_SQL);
            jdbc.update("DELETE FROM " + TABLE);
            if (!rows.isEmpty()) {
                jdbc.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int index) throws SQLException {
                        WriteRow row = rows.get(index);
                        MetricStrategySnapshotDto dto = row.dto();
                        ps.setString(1, dto.getStrategyKey());
                        ps.setString(2, row.type().name());
                        ps.setString(3, dto.getGenerationId());
                        ps.setInt(4, dto.getMetricVersion());
                        ps.setString(5, dto.getSourceVersion());
                        ps.setString(6, dto.getWalletId());
                        ps.setString(7, dto.getStrategyCode());
                        ps.setString(8, dto.getScopeType());
                        ps.setString(9, dto.getScopeValue());
                        ps.setObject(10, dto.getComputedAt());
                        ps.setObject(11, dto.getDataAsOf());
                        ps.setTimestamp(12, Timestamp.from(row.fetchedAt()));
                        ps.setTimestamp(13, Timestamp.from(row.fetchedAt().plus(ttl)));
                        ps.setBoolean(14, dto.isDecisionFinal());
                        ps.setBoolean(15, dto.isAllowNewEntries());
                        ps.setString(16, json(dto.getReasonCodes()));
                        ps.setString(17, json(dto));
                    }

                    @Override
                    public int getBatchSize() {
                        return rows.size();
                    }
                });
            }
        });
        log.debug("event=metric_v2.snapshot.persisted rows={} atomicReplace=true", rows.size());
    }

    private List<WriteRow> flatten(MetricV2SnapshotStore.Snapshot snapshot) {
        List<WriteRow> rows = new ArrayList<>();
        append(rows, SnapshotType.SUMMARY, snapshot.summaryByKey(), snapshot.summaryFetchedAt());
        append(rows, SnapshotType.FULL, snapshot.fullByKey(), snapshot.fullFetchedAt());
        append(rows, SnapshotType.COPY_GUARD, snapshot.guardByKey(), snapshot.guardFetchedAt());
        return rows;
    }

    private static void append(
            List<WriteRow> target,
            SnapshotType type,
            Map<String, MetricStrategySnapshotDto> values,
            Instant fetchedAt
    ) {
        if (values == null || values.isEmpty()) return;
        if (fetchedAt == null) throw new IllegalStateException("METRIC_V2_FETCHED_AT_REQUIRED:" + type);
        values.values().stream()
                .sorted(java.util.Comparator.comparing(MetricStrategySnapshotDto::getStrategyKey))
                .map(dto -> new WriteRow(type, dto, fetchedAt))
                .forEach(target::add);
    }

    private MetricStrategySnapshotDto readDto(String json) {
        try {
            return objectMapper.readValue(json, MetricStrategySnapshotDto.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("METRIC_V2_PERSISTED_PAYLOAD_INVALID", ex);
        }
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("METRIC_V2_SNAPSHOT_SERIALIZATION_FAILED", ex);
        }
    }

    private static Instant latest(Instant left, Instant right) {
        if (left == null) return right;
        return left.isAfter(right) ? left : right;
    }

    private enum SnapshotType {
        SUMMARY,
        FULL,
        COPY_GUARD
    }

    private record PersistedRow(SnapshotType type, MetricStrategySnapshotDto dto, Instant fetchedAt) {
    }

    private record WriteRow(SnapshotType type, MetricStrategySnapshotDto dto, Instant fetchedAt) {
    }
}
