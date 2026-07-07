package com.apunto.engine.repository;

import com.apunto.engine.entity.UserApiKeyEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.UUID;

public interface UserApiKeyRepository extends JpaRepository<UserApiKeyEntity, UUID> {


    UserApiKeyEntity findByUser_Id(UUID userId);

    @Query(value = """
            select count(distinct user_id)
            from futuros_operaciones.user_api_keys
            where nullif(trim(api_key), '') is not null
              and nullif(trim(api_secret), '') is not null
            """, nativeQuery = true)
    long countUsersWithUsableApiKeys();
}
