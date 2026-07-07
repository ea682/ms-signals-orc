package com.apunto.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "copy_promotion_audit", schema = "futuros_operaciones")
public class CopyPromotionAuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "id_user")
    private UUID idUser;

    @Column(name = "wallet_id", length = 128)
    private String walletId;

    @Column(name = "copy_strategy_code", length = 64)
    private String copyStrategyCode;

    @Column(name = "source_execution_mode", length = 32)
    private String sourceExecutionMode;

    @Column(name = "target_execution_mode", length = 32)
    private String targetExecutionMode;

    @Column(name = "decision", nullable = false, length = 64)
    private String decision;

    @Column(name = "reason_code", nullable = false, length = 120)
    private String reasonCode;

    @Builder.Default
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "reason_details", nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> reasonDetails = new LinkedHashMap<>();

    @Column(name = "shadow_allocation_id")
    private Long shadowAllocationId;

    @Column(name = "micro_live_allocation_id")
    private Long microLiveAllocationId;

    @Column(name = "live_allocation_id")
    private Long liveAllocationId;

    @Column(name = "created_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime createdAt;

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = OffsetDateTime.now();
        normalize();
    }

    private void normalize() {
        walletId = walletId == null ? null : walletId.trim().toLowerCase(Locale.ROOT);
        copyStrategyCode = upperOrNull(copyStrategyCode);
        sourceExecutionMode = upperOrNull(sourceExecutionMode);
        targetExecutionMode = upperOrNull(targetExecutionMode);
        decision = upperOrNull(decision);
        reasonCode = upperOrNull(reasonCode);
        if (decision == null) decision = "SHADOW_EVALUATED";
        if (reasonCode == null) reasonCode = "UNKNOWN";
        if (reasonDetails == null) reasonDetails = new LinkedHashMap<>();
    }

    private static String upperOrNull(String raw) {
        if (raw == null || raw.isBlank()) return null;
        return raw.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }
}
