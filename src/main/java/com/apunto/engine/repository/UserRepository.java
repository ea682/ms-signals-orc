package com.apunto.engine.repository;

import com.apunto.engine.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface UserRepository extends JpaRepository<UserEntity, UUID> {

    List<UserEntity> findAll();

    @Query("select count(u) from UserEntity u where u.activo = true")
    long countActiveUsers();
}
