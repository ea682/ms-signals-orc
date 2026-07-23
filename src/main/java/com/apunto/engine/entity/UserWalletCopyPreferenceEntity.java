package com.apunto.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.Locale;
import java.util.UUID;

@Entity
@Table(name = "user_wallet_copy_preference", schema = "futuros_operaciones")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserWalletCopyPreferenceEntity {

    @Id
    private UUID id;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "wallet_id", nullable = false, length = 160)
    private String walletId;

    @Column(name = "wallet_blocked", nullable = false)
    private boolean walletBlocked;

    @Column(name = "created_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now();
        if (id == null) id = UUID.randomUUID();
        if (createdAt == null) createdAt = now;
        if (updatedAt == null) updatedAt = now;
        normalizeWallet();
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = OffsetDateTime.now();
        normalizeWallet();
    }

    private void normalizeWallet() {
        if (walletId != null) walletId = walletId.trim().toLowerCase(Locale.ROOT);
    }
}

