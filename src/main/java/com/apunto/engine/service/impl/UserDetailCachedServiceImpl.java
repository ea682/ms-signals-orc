package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.UserDetailService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class UserDetailCachedServiceImpl implements UserDetailCachedService {

    private static final Duration TTL = Duration.ofMinutes(5);
    private static final UserSnapshot EMPTY_SNAPSHOT = new UserSnapshot(List.of(), Map.of(), null);

    private final UserDetailService userDetailService;

    private volatile UserSnapshot snapshot = EMPTY_SNAPSHOT;

    @Override
    public List<UserDetailDto> getUsers() {
        return currentSnapshot().users();
    }

    @Override
    public Optional<UserDetailDto> getUserById(String userId) {
        if (userId == null || userId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(currentSnapshot().byUserId().get(userId));
    }

    private UserSnapshot currentSnapshot() {
        UserSnapshot current = snapshot;
        if (!isExpired(current)) {
            return current;
        }
        return refreshSnapshotIfNeeded();
    }

    @Override
    public synchronized void updateRuntimeCapital(UUID userId, Integer capital, String capitalAsset) {
        if (userId == null || snapshot == null || snapshot.byUserId() == null) {
            return;
        }
        UserDetailDto userDetail = snapshot.byUserId().get(userId.toString());
        if (userDetail == null || userDetail.getDetail() == null) {
            return;
        }
        if (capital != null) {
            userDetail.getDetail().setCapital(Math.max(0, capital));
        }
        if (capitalAsset != null && !capitalAsset.isBlank()) {
            userDetail.getDetail().setCapitalAsset(capitalAsset.trim().toUpperCase(java.util.Locale.ROOT));
        }
    }

    private synchronized UserSnapshot refreshSnapshotIfNeeded() {
        UserSnapshot current = snapshot;
        if (!isExpired(current)) {
            return current;
        }

        final List<UserDetailDto> loadedUsers = Optional.ofNullable(userDetailService.findAllActive()).orElse(List.of());
        final List<UserDetailDto> users = loadedUsers.stream()
                .filter(Objects::nonNull)
                .toList();
        final Map<String, UserDetailDto> byUserId = new HashMap<>(Math.max(16, users.size() * 2));

        for (UserDetailDto userDetail : users) {
            if (userDetail == null || userDetail.getUser() == null || userDetail.getUser().getId() == null) {
                continue;
            }
            byUserId.put(userDetail.getUser().getId().toString(), userDetail);
        }

        final UserSnapshot refreshed = new UserSnapshot(List.copyOf(users), Map.copyOf(byUserId), Instant.now());
        snapshot = refreshed;
        return refreshed;
    }

    private boolean isExpired(UserSnapshot current) {
        return current == null
                || current.lastUpdate() == null
                || Duration.between(current.lastUpdate(), Instant.now()).compareTo(TTL) >= 0;
    }

    private record UserSnapshot(List<UserDetailDto> users,
                                Map<String, UserDetailDto> byUserId,
                                Instant lastUpdate) {
        private UserSnapshot {
            users = users == null ? List.of() : users;
            byUserId = byUserId == null ? Map.of() : byUserId;
            Objects.requireNonNull(users, "users");
            Objects.requireNonNull(byUserId, "byUserId");
        }
    }
}
