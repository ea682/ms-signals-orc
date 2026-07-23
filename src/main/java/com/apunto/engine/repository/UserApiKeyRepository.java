package com.apunto.engine.repository;

import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.UUID;
import java.util.Optional;

public interface UserApiKeyRepository extends JpaRepository<UserApiKeyEntity, UUID> {

    UserApiKeyEntity findFirstByUser_IdAndExchangeIgnoreCaseOrderByUpdatedAtDesc(UUID userId, String exchange);

    Optional<UserApiKeyEntity> findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(
            UUID userId, String exchange, ExecutionAccountPurpose accountPurpose);

    Optional<UserApiKeyEntity> findByUser_IdAndExchangeIgnoreCaseAndAccountPurpose(
            UUID userId, String exchange, ExecutionAccountPurpose accountPurpose);

    default UserApiKeyEntity findByUser_Id(UUID userId) {
        return findFirstByUser_IdAndExchangeIgnoreCaseOrderByUpdatedAtDesc(userId, "BINANCE");
    }

    @Query(value = """
            select count(distinct user_id)
            from futuros_operaciones.user_api_keys
            where nullif(trim(api_key), '') is not null
              and nullif(trim(api_secret), '') is not null
              and upper(trim(exchange)) = 'BINANCE'
            """, nativeQuery = true)
    long countUsersWithUsableApiKeys();
}
