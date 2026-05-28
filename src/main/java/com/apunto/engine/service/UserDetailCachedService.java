package com.apunto.engine.service;

import com.apunto.engine.dto.UserDetailDto;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface UserDetailCachedService {
    List<UserDetailDto> getUsers();

    Optional<UserDetailDto> getUserById(String userId);

    void updateRuntimeCapital(UUID userId, Integer capital, String capitalAsset);
}

