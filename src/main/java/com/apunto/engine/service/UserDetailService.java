package com.apunto.engine.service;

import com.apunto.engine.dto.UserDetailDto;

import java.util.List;

public interface UserDetailService {
    List<UserDetailDto> findAll();
}
