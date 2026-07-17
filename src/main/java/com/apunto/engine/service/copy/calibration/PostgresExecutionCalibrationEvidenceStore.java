package com.apunto.engine.service.copy.calibration;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class PostgresExecutionCalibrationEvidenceStore implements ExecutionCalibrationEvidenceStore {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public List<CalibrationObservation> find(String strategyKey,
                                             String generationId,
                                             ExecutionCalibrationMetric metric) {
        String sql = """
                SELECT
                    COALESCE(symbol, 'ALL') AS symbol,
                    COALESCE(position_side, 'ALL') AS position_side,
                    COALESCE(copy_action, 'ALL') AS copy_action,
                    COALESCE(notional_band, 'ALL') AS notional_band,
                    %s AS observed_value,
                    COALESCE(newest_sample_at, updated_at) AS observed_at,
                    COALESCE(calibration_version, slippage_model_version, 'UNKNOWN') AS model_version,
                    GREATEST(sample_count, 1) AS sample_count
                FROM futuros_operaciones.copy_micro_live_calibration_v3
                WHERE strategy_key = ?
                  AND generation_id::text = ?
                  AND status = 'CALIBRATED'
                  AND %s IS NOT NULL
                ORDER BY COALESCE(newest_sample_at, updated_at), calibration_key
                """.formatted(metric.columnName(), metric.columnName());
        return jdbcTemplate.query(sql, (rs, rowNumber) -> new CalibrationObservation(
                new CalibrationSegment(
                        rs.getString("symbol"),
                        rs.getString("position_side"),
                        rs.getString("copy_action"),
                        rs.getString("notional_band")),
                rs.getBigDecimal("observed_value"),
                rs.getObject("observed_at", OffsetDateTime.class),
                rs.getString("model_version"),
                rs.getInt("sample_count")), strategyKey, generationId);
    }
}
