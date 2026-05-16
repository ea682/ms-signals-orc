package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;
import com.apunto.engine.repository.CopyOperationRepository;
import com.apunto.engine.service.ActiveCopyOperationCache;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ActiveCopyOperationCacheImpl implements ActiveCopyOperationCache {

    private final CopyOperationRepository repository;
    private final ConcurrentMap<String, ActiveCopyRef> activeByOriginUser = new ConcurrentHashMap<>();

    public ActiveCopyOperationCacheImpl(CopyOperationRepository repository) {
        this.repository = repository;
    }

    @PostConstruct
    public void start() {
        refreshFromDatabase("startup");
    }

    @Scheduled(fixedDelayString = "${copy.operation.active-cache.refresh-ms:60000}")
    public void scheduledRefresh() {
        refreshFromDatabase("scheduled");
    }

    @Override
    public boolean isActive(String originId, String userId) {
        String key = key(originId, userId);
        return key != null && activeByOriginUser.containsKey(key);
    }

    @Override
    public Set<String> activeUserIds(String originId) {
        String normalizedOrigin = normalize(originId);
        if (normalizedOrigin == null) {
            return Collections.emptySet();
        }
        return activeByOriginUser.values().stream()
                .filter(ref -> normalizedOrigin.equals(normalize(ref.originId())))
                .map(ActiveCopyRef::userId)
                .filter(v -> v != null && !v.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<String> activeUserIdsByWallet(String walletId) {
        String normalizedWallet = normalize(walletId);
        if (normalizedWallet == null) {
            return Collections.emptySet();
        }
        return activeByOriginUser.values().stream()
                .filter(ref -> normalizedWallet.equals(normalize(ref.wallet())))
                .map(ActiveCopyRef::userId)
                .filter(v -> v != null && !v.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<String> activeUserIdsByWalletAndSymbol(String walletId, String symbol) {
        String normalizedWallet = normalize(walletId);
        String normalizedSymbol = normalize(symbol);
        if (normalizedWallet == null || normalizedSymbol == null) {
            return Collections.emptySet();
        }
        return activeByOriginUser.values().stream()
                .filter(ref -> normalizedWallet.equals(normalize(ref.wallet())))
                .filter(ref -> normalizedSymbol.equals(normalize(ref.symbol())))
                .map(ActiveCopyRef::userId)
                .filter(v -> v != null && !v.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public void markOpen(CopyOperationDto operation) {
        if (operation == null || !operation.isActive()) {
            return;
        }
        String key = key(operation.getIdOrderOrigin(), operation.getIdUser());
        if (key == null) {
            return;
        }
        activeByOriginUser.put(key, ActiveCopyRef.from(operation));
        log.debug("event=copy_operation.active_cache.mark_open originId={} userId={} wallet={} symbol={} activeSize={}",
                safeLog(operation.getIdOrderOrigin()), safeLog(operation.getIdUser()), safeLog(operation.getIdWalletOrigin()),
                safeLog(operation.getParsymbol()), activeByOriginUser.size());
    }

    @Override
    public void markClosed(String originId, String userId) {
        String key = key(originId, userId);
        if (key == null) {
            return;
        }
        ActiveCopyRef removed = activeByOriginUser.remove(key);
        log.debug("event=copy_operation.active_cache.mark_closed originId={} userId={} removed={} activeSize={}",
                safeLog(originId), safeLog(userId), removed != null, activeByOriginUser.size());
    }

    @Override
    public int activeSize() {
        return activeByOriginUser.size();
    }

    private void refreshFromDatabase(String trigger) {
        long startedNs = System.nanoTime();
        try {
            List<CopyOperationEntity> active = repository.findAllByActiveTrue();
            ConcurrentMap<String, ActiveCopyRef> next = new ConcurrentHashMap<>();
            Set<String> duplicatedKeys = new HashSet<>();
            for (CopyOperationEntity entity : active) {
                if (entity == null) {
                    continue;
                }
                String key = key(entity.getIdOrderOrigin(), entity.getIdUser());
                if (key == null) {
                    continue;
                }
                ActiveCopyRef previous = next.put(key, ActiveCopyRef.from(entity));
                if (previous != null) {
                    duplicatedKeys.add(key);
                }
            }
            activeByOriginUser.clear();
            activeByOriginUser.putAll(next);
            log.info("event=copy_operation.active_cache.refresh_ok trigger={} dbActive={} cached={} duplicateKeys={} elapsedMs={}",
                    trigger, active.size(), activeByOriginUser.size(), duplicatedKeys.size(), elapsedMs(startedNs));
        } catch (DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            log.warn("event=copy_operation.active_cache.refresh_failed trigger={} cached={} errClass={} errMsg=\"{}\" elapsedMs={}",
                    trigger, activeByOriginUser.size(), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
        }
    }

    private String key(String originId, String userId) {
        String origin = normalize(originId);
        String user = normalize(userId);
        if (origin == null || user == null) {
            return null;
        }
        return origin + '|' + user;
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }

    private record ActiveCopyRef(
            String originId,
            String userId,
            String wallet,
            String symbol,
            String side
    ) {
        static ActiveCopyRef from(CopyOperationDto dto) {
            return new ActiveCopyRef(
                    dto.getIdOrderOrigin(),
                    dto.getIdUser(),
                    dto.getIdWalletOrigin(),
                    dto.getParsymbol(),
                    dto.getTypeOperation()
            );
        }

        static ActiveCopyRef from(CopyOperationEntity entity) {
            return new ActiveCopyRef(
                    entity.getIdOrderOrigin(),
                    entity.getIdUser(),
                    entity.getIdWalletOrigin(),
                    entity.getParsymbol(),
                    entity.getTypeOperation()
            );
        }
    }
}
