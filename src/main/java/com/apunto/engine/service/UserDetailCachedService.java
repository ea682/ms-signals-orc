package com.apunto.engine.service;

import com.apunto.engine.dto.UserDetailDto;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface UserDetailCachedService {
    List<UserDetailDto> getUsers();

    default List<UserDetailDto> getUsersCachedOnly() {
        return List.of();
    }

    Optional<UserDetailDto> getUserById(String userId);

    default Optional<UserDetailDto> getUserByIdCachedOnly(String userId) {
        return Optional.empty();
    }

    void updateRuntimeCapital(UUID userId, Integer capital, String capitalAsset);
}
