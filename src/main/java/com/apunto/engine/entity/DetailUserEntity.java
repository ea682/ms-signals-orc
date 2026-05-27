package com.apunto.engine.entity;

import com.apunto.engine.shared.enums.CopyMinNotionalMode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "detail_user", schema = "futuros_operaciones")
@Data
public class DetailUserEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id_detail_usr", nullable = false, updatable = false)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "id_users", nullable = false)
    private UserEntity user;

    @Column(name = "is_user_active", nullable = false)
    private boolean userActive = false;

    @Column(name = "is_api_key_binance_active", nullable = false)
    private boolean apiKeyBinar = false;

    @Column(name = "leverage", nullable = false)
    private Integer leverage = 1;

    @Column(name = "capital", nullable = false)
    private Integer capital = 1000;

    @Column(name = "capital_asset", nullable = false, length = 4)
    private String capitalAsset = "USDT";

    @Column(name = "max_wallet", nullable = false)
    private Integer maxWallet = 1;

    @Enumerated(EnumType.STRING)
    @Column(name = "copy_min_notional_mode", nullable = false, length = 32)
    private CopyMinNotionalMode copyMinNotionalMode = CopyMinNotionalMode.SKIP;

    @Column(name = "copy_min_notional_max_usdt", precision = 18, scale = 8)
    private BigDecimal copyMinNotionalMaxUsdt;

    @Column(name = "copy_min_notional_min_score", nullable = false)
    private Integer copyMinNotionalMinScore = 0;

    @Column(name = "copy_min_notional_min_history_days", nullable = false)
    private Integer copyMinNotionalMinHistoryDays = 0;

    @Column(name = "copy_min_notional_min_operations", nullable = false)
    private Integer copyMinNotionalMinOperations = 0;
}
