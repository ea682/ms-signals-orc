package com.apunto.engine.entity;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "users", schema = "futuros_operaciones")
@Data
public class UserEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "email", nullable = false, length = 320)
    private String email;

    @Column(name = "nombre", length = 255)
    private String nombre;

    @Column(name = "password_hash")
    private String passwordHash;

    @Column(name = "google_sub")
    private String googleSub;

    @Column(name = "google_email", length = 320)
    private String googleEmail;

    @Column(name = "google_picture")
    private String googlePicture;

    @Column(name = "email_verificado", nullable = false)
    private boolean emailVerificado = false;

    @Column(name = "activo", nullable = false)
    private boolean activo = true;

    @Column(name = "ultimo_login_at")
    private OffsetDateTime ultimoLoginAt;

    @CreationTimestamp
    @Column(name = "creado_at", nullable = false, updatable = false)
    private OffsetDateTime creadoAt;

    @UpdateTimestamp
    @Column(name = "actualizado_at", nullable = false)
    private OffsetDateTime actualizadoAt;

}
