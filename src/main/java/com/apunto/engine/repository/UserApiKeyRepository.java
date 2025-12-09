package com.apunto.engine.repository;

import com.apunto.engine.entity.UserApiKeyEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface UserApiKeyRepository extends JpaRepository<UserApiKeyEntity, Long> {


    UserApiKeyEntity findByUser_Id(UUID userId);
}
