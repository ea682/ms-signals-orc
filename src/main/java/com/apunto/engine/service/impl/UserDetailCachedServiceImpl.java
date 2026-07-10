package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.UserDetailService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
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
@Slf4j
public class UserDetailCachedServiceImpl implements UserDetailCachedService {

    private static final Duration TTL = Duration.ofMinutes(5);
    private static final UserSnapshot EMPTY_SNAPSHOT = new UserSnapshot(List.of(), Map.of(), null);

    private final UserDetailService userDetailService;

    @Value("${copy.runtime.user-snapshot.max-stale:PT10M}")
    private Duration maxStale;

    private volatile UserSnapshot snapshot = EMPTY_SNAPSHOT;

    @Override
    public List<UserDetailDto> getUsers() {
        return currentSnapshot().users();
    }

    @Override
    public List<UserDetailDto> getUsersCachedOnly() {
        UserSnapshot current = snapshot;
        if (isTooStale(current)) {
            log.warn("event=copy.runtime.user_snapshot.unavailable reasonCode=USER_SNAPSHOT_MISSING_OR_STALE decision=FAIL_CLOSED");
            return List.of();
        }
        return current.users();
    }

    @Override
    public Optional<UserDetailDto> getUserById(String userId) {
        if (userId == null || userId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(currentSnapshot().byUserId().get(userId));
    }

    @Override
    public Optional<UserDetailDto> getUserByIdCachedOnly(String userId) {
        if (userId == null || userId.isBlank()) return Optional.empty();
        UserSnapshot current = snapshot;
        return isTooStale(current) ? Optional.empty()
                : Optional.ofNullable(current.byUserId().get(userId));
    }

    @Scheduled(initialDelayString = "${copy.runtime.user-snapshot.initial-delay-ms:0}",
            fixedDelayString = "${copy.runtime.user-snapshot.refresh-ms:60000}")
    public void refreshSnapshotOutOfBand() {
        refreshSnapshot(true);
    }

    private UserSnapshot currentSnapshot() {
        UserSnapshot current = snapshot;
        if (!isExpired(current)) {
            return current;
        }
        return refreshSnapshot(false);
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

    private synchronized UserSnapshot refreshSnapshot(boolean force) {
        UserSnapshot current = snapshot;
        if (!force && !isExpired(current)) {
            return current;
        }

        try {
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
            log.info("event=copy.runtime.user_snapshot.refreshed users={} source=scheduled_out_of_band", users.size());
            return refreshed;
        } catch (RuntimeException ex) {
            log.error("event=copy.runtime.user_snapshot.refresh_failed reasonCode=USER_SNAPSHOT_REFRESH_FAILED decision=KEEP_LAST_KNOWN_GOOD errClass={} errMsg=\"{}\"",
                    ex.getClass().getSimpleName(), safe(ex.getMessage()));
            return current == null ? EMPTY_SNAPSHOT : current;
        }
    }

    private boolean isExpired(UserSnapshot current) {
        return current == null
                || current.lastUpdate() == null
                || Duration.between(current.lastUpdate(), Instant.now()).compareTo(TTL) >= 0;
    }

    private boolean isTooStale(UserSnapshot current) {
        Duration allowed = maxStale == null || maxStale.isNegative() || maxStale.isZero()
                ? Duration.ofMinutes(10) : maxStale;
        return current == null || current.lastUpdate() == null
                || Duration.between(current.lastUpdate(), Instant.now()).compareTo(allowed) > 0;
    }

    private String safe(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
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
