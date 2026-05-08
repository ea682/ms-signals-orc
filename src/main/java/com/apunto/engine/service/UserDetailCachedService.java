package com.apunto.engine.service;

import com.apunto.engine.dto.UserDetailDto;

import java.util.List;
import java.util.Optional;

public interface UserDetailCachedService {
    List<UserDetailDto> getUsers();

    Optional<UserDetailDto> getUserById(String userId);
}
