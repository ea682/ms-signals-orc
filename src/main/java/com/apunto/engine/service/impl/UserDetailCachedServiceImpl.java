package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.UserDetailService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class UserDetailCachedServiceImpl implements UserDetailCachedService {

    private final UserDetailService userDetailService;

    private List<UserDetailDto> cache;
    private Instant lastUpdate;

    private static final Duration TTL = Duration.ofMinutes(5);

    @Override
    public synchronized List<UserDetailDto> getUsers() {
        if (cache == null || isExpired()) {
            cache = userDetailService.findAll();
            lastUpdate = Instant.now();
        }
        return cache;
    }

    private boolean isExpired() {
        if (lastUpdate == null) return true;
        return Duration.between(lastUpdate, Instant.now()).compareTo(TTL) >= 0;
    }
}