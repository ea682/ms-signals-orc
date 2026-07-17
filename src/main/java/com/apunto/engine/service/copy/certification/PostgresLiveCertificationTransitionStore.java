package com.apunto.engine.service.copy.certification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class PostgresLiveCertificationTransitionStore implements LiveCertificationTransitionStore {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public Optional<LiveCertificationSnapshot> lockById(UUID certificationId) {
        return jdbcTemplate.query("""
                        SELECT id, certification_status, version
                        FROM strategy_live_certification
                        WHERE id = ?
                        FOR UPDATE
                        """,
                (rs, rowNum) -> new LiveCertificationSnapshot(
                        rs.getObject("id", UUID.class),
                        LiveCertificationStatus.valueOf(rs.getString("certification_status")),
                        rs.getLong("version")), certificationId).stream().findFirst();
    }

    @Override
    public Optional<LiveCertificationAuditFact> findAuditByTransitionKey(String transitionKey) {
        return jdbcTemplate.query("""
                        SELECT certification_id, transition_key, prior_status, next_status,
                               prior_version, next_version, actor, reason,
                               evidence_snapshot::text AS evidence_snapshot, occurred_at
                        FROM strategy_live_certification_audit
                        WHERE transition_key = ?
                        """,
                (rs, rowNum) -> new LiveCertificationAuditFact(
                        rs.getObject("certification_id", UUID.class),
                        rs.getString("transition_key"),
                        LiveCertificationStatus.valueOf(rs.getString("prior_status")),
                        LiveCertificationStatus.valueOf(rs.getString("next_status")),
                        rs.getLong("prior_version"), rs.getLong("next_version"),
                        rs.getString("actor"), rs.getString("reason"),
                        readMap(rs.getString("evidence_snapshot")),
                        rs.getObject("occurred_at", OffsetDateTime.class)),
                transitionKey).stream().findFirst();
    }

    @Override
    public boolean compareAndSet(UUID certificationId, long expectedVersion,
                                 LiveCertificationStatus expectedStatus,
                                 LiveCertificationStatus nextStatus) {
        return jdbcTemplate.update("""
                        UPDATE strategy_live_certification
                        SET certification_status = ?, version = version + 1, updated_at = now()
                        WHERE id = ? AND version = ? AND certification_status = ?
                        """,
                nextStatus.name(), certificationId, expectedVersion, expectedStatus.name()) == 1;
    }

    @Override
    public void appendAudit(LiveCertificationAuditFact audit) {
        jdbcTemplate.update("""
                        INSERT INTO strategy_live_certification_audit (
                            id, certification_id, transition_key, prior_status, next_status,
                            prior_version, next_version, actor, reason, evidence_snapshot, occurred_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?)
                        """,
                UUID.randomUUID(), audit.certificationId(), audit.transitionKey(),
                audit.priorStatus().name(), audit.nextStatus().name(), audit.priorVersion(),
                audit.nextVersion(), audit.actor(), audit.reason(), writeMap(audit.evidenceSnapshot()),
                audit.occurredAt());
    }

    private Map<String, Object> readMap(String json) {
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Invalid certification audit JSON", ex);
        }
    }

    private String writeMap(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Certification evidence is not serializable", ex);
        }
    }
}
