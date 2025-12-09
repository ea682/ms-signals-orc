package com.apunto.engine.entity;


import jakarta.persistence.*;
import lombok.Data;

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

    @Column(name = "max_wallet", nullable = false)
    private Integer maxWallet = 1;
}
