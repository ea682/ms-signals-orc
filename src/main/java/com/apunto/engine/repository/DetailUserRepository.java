package com.apunto.engine.repository;

import com.apunto.engine.entity.DetailUserEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface DetailUserRepository extends JpaRepository<DetailUserEntity, UUID> {

    DetailUserEntity findByUser_Id(UUID userId);
}
