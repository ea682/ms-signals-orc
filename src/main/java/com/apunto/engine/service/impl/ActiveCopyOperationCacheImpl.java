package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;
import com.apunto.engine.mapper.CopyOperationMapper;
import com.apunto.engine.repository.CopyOperationRepository;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.shared.util.CopySymbolIdentity;
import com.apunto.engine.shared.util.CopyTraceIdUtil;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ActiveCopyOperationCacheImpl implements ActiveCopyOperationCache {

    private final CopyOperationRepository repository;
    private final CopyOperationMapper mapper;
    private final ConcurrentMap<String, RuntimeCopyRef> byRuntimeKey = new ConcurrentHashMap<>();

    @Value("${copy.operation.active-cache.pending-ttl-ms:120000}")
    private long pendingTtlMs;

    public ActiveCopyOperationCacheImpl(CopyOperationRepository repository, CopyOperationMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
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
        return byRuntimeKey.values().stream()
                .anyMatch(ref -> matchesOriginUser(ref, originId, userId) && ref.status() == RuntimeCopyStatus.ACTIVE);
    }

    @Override
    public boolean isActive(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation) {
        RuntimeCopyRef ref = byRuntimeKey.get(key(originId, userId, allocationId, strategyCode, symbol, typeOperation));
        return ref != null && ref.status() == RuntimeCopyStatus.ACTIVE;
    }

    @Override
    public boolean isKnown(String originId, String userId) {
        return byRuntimeKey.values().stream()
                .anyMatch(ref -> matchesOriginUser(ref, originId, userId) && !ref.isExpiredNonActive(pendingTtlMs));
    }

    @Override
    public boolean isKnown(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation) {
        RuntimeCopyRef ref = byRuntimeKey.get(key(originId, userId, allocationId, strategyCode, symbol, typeOperation));
        return ref != null && !ref.isExpiredNonActive(pendingTtlMs);
    }

    @Override
    public CopyOperationDto activeOperation(String originId, String userId) {
        return byRuntimeKey.values().stream()
                .filter(ref -> matchesOriginUser(ref, originId, userId))
                .filter(ref -> ref.status() == RuntimeCopyStatus.ACTIVE)
                .map(RuntimeCopyRef::operation)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    @Override
    public CopyOperationDto activeOperation(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation) {
        RuntimeCopyRef ref = byRuntimeKey.get(key(originId, userId, allocationId, strategyCode, symbol, typeOperation));
        return ref == null || ref.status() != RuntimeCopyStatus.ACTIVE ? null : ref.operation();
    }

    @Override
    public List<CopyOperationDto> activeOperations(String originId, String userId) {
        return byRuntimeKey.values().stream()
                .filter(ref -> matchesOriginUser(ref, originId, userId))
                .filter(ref -> ref.status() == RuntimeCopyStatus.ACTIVE)
                .map(RuntimeCopyRef::operation)
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    public List<CopyOperationDto> activeOperationsByUserAndWallet(String userId, String walletId) {
        String normalizedUser = normalize(userId);
        String normalizedWallet = normalize(walletId);
        if (normalizedUser == null || normalizedWallet == null) {
            return List.of();
        }
        return byRuntimeKey.values().stream()
                .filter(ref -> ref.status() == RuntimeCopyStatus.ACTIVE)
                .filter(ref -> normalizedUser.equals(normalize(ref.userId())))
                .filter(ref -> normalizedWallet.equals(normalize(ref.wallet())))
                .map(RuntimeCopyRef::operation)
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    public Set<String> activeUserIds(String originId) {
        String normalizedOrigin = normalize(originId);
        if (normalizedOrigin == null) {
            return Collections.emptySet();
        }
        return byRuntimeKey.values().stream()
                .filter(ref -> ref.status() == RuntimeCopyStatus.ACTIVE)
                .filter(ref -> normalizedOrigin.equals(normalize(ref.originId())))
                .map(RuntimeCopyRef::userId)
                .filter(v -> v != null && !v.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<String> activeUserIdsByWallet(String walletId) {
        String normalizedWallet = normalize(walletId);
        if (normalizedWallet == null) {
            return Collections.emptySet();
        }
        return byRuntimeKey.values().stream()
                .filter(ref -> ref.status() == RuntimeCopyStatus.ACTIVE)
                .filter(ref -> normalizedWallet.equals(normalize(ref.wallet())))
                .map(RuntimeCopyRef::userId)
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
        return byRuntimeKey.values().stream()
                .filter(ref -> ref.status() == RuntimeCopyStatus.ACTIVE)
                .filter(ref -> normalizedWallet.equals(normalize(ref.wallet())))
                .filter(ref -> normalizedSymbol.equals(normalize(ref.symbol())))
                .map(RuntimeCopyRef::userId)
                .filter(v -> v != null && !v.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<String> activeUserIdsByWalletAndBaseSymbol(String walletId, String symbol) {
        String normalizedWallet = normalize(walletId);
        if (normalizedWallet == null || CopySymbolIdentity.primaryBaseAsset(symbol) == null) {
            return Collections.emptySet();
        }
        return byRuntimeKey.values().stream()
                .filter(ref -> ref.status() == RuntimeCopyStatus.ACTIVE)
                .filter(ref -> normalizedWallet.equals(normalize(ref.wallet())))
                .filter(ref -> CopySymbolIdentity.sameBaseAsset(ref.symbol(), symbol))
                .map(RuntimeCopyRef::userId)
                .filter(v -> v != null && !v.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public String traceId(String originId, String userId, String walletId, String symbol) {
        RuntimeCopyRef existing = byRuntimeKey.values().stream()
                .filter(ref -> matchesOriginUser(ref, originId, userId))
                .filter(ref -> Objects.equals(normalize(ref.wallet()), normalize(walletId)))
                .filter(ref -> CopySymbolIdentity.sameBaseAsset(ref.symbol(), symbol))
                .findFirst()
                .orElse(null);
        if (existing != null && existing.traceId() != null && !existing.traceId().isBlank()) return existing.traceId();
        return CopyTraceIdUtil.copyTraceId(originId, userId, walletId, symbol);
    }

    @Override
    public void markPendingOpen(String originId, String userId, String walletId, String symbol, String typeOperation, String traceId) {
        markPendingOpen(originId, userId, walletId, symbol, typeOperation, null, null, traceId);
    }

    @Override
    public String traceId(String originId, String userId, String walletId, String symbol, Long allocationId, String strategyCode) {
        RuntimeCopyRef existing = byRuntimeKey.values().stream()
                .filter(ref -> matchesOriginUser(ref, originId, userId))
                .filter(ref -> sameAllocationScope(ref, allocationId, strategyCode))
                .filter(ref -> Objects.equals(normalize(ref.wallet()), normalize(walletId)))
                .filter(ref -> CopySymbolIdentity.sameBaseAsset(ref.symbol(), symbol))
                .findFirst()
                .orElse(null);
        if (existing != null && existing.traceId() != null && !existing.traceId().isBlank()) return existing.traceId();
        return CopyTraceIdUtil.copyTraceId(originId, userId, walletId, symbol);
    }

    @Override
    public void markPendingOpen(String originId, String userId, String walletId, String symbol, String typeOperation, Long allocationId, String strategyCode, String traceId) {
        String key = key(originId, userId, allocationId, strategyCode, symbol, typeOperation);
        if (key == null) {
            return;
        }
        RuntimeCopyRef next = RuntimeCopyRef.pending(originId, userId, walletId, symbol, typeOperation, allocationId, strategyCode, traceId);
        byRuntimeKey.put(key, next);
        log.info("event=copy_runtime_state.pending_open category=runtime_state reasonAlias=open_in_progress friendlyReason=apertura_en_proceso explanation=la_operacion_queda_en_ram_para_evitar_duplicados_mientras_se_envia_a_binance copyImpact=blocks_duplicate_open traceId={} originId={} userId={} allocationId={} strategy={} wallet={} symbol={} typeOperation={} runtimeSize={}",
                safeLog(next.traceId()), safeLog(originId), safeLog(userId), allocationId, safeLog(strategyCode), safeLog(walletId), safeLog(symbol), safeLog(typeOperation), byRuntimeKey.size());
    }

    @Override
    public void markOpen(CopyOperationDto operation) {
        if (operation == null || !operation.isActive()) {
            return;
        }
        String key = key(operation);
        if (key == null) {
            return;
        }
        String traceId = traceId(operation.getIdOrderOrigin(), operation.getIdUser(), operation.getIdWalletOrigin(), operation.getParsymbol(), operation.getUserCopyAllocationId(), operation.getCopyStrategyCode());
        RuntimeCopyRef next = RuntimeCopyRef.active(operation, traceId);
        byRuntimeKey.put(key, next);
        log.info("event=copy_runtime_state.active category=runtime_state reasonAlias=copy_active friendlyReason=copia_activa_en_ram explanation=la_copia_quedo_disponible_en_ram_para_la_ruta_caliente copyImpact=copy_tracked traceId={} originId={} userId={} allocationId={} strategy={} wallet={} symbol={} typeOperation={} qty={} runtimeSize={}",
                safeLog(traceId), safeLog(operation.getIdOrderOrigin()), safeLog(operation.getIdUser()),
                operation.getUserCopyAllocationId(), safeLog(operation.getCopyStrategyCode()), safeLog(operation.getIdWalletOrigin()),
                safeLog(operation.getParsymbol()), safeLog(operation.getTypeOperation()), operation.getSizePar(), byRuntimeKey.size());
    }

    @Override
    public void markUncertain(CopyOperationDto operation, String traceId, String reasonCode) {
        if (operation == null) {
            return;
        }
        String key = key(operation);
        if (key == null) {
            return;
        }
        RuntimeCopyRef next = RuntimeCopyRef.uncertain(operation, traceId);
        byRuntimeKey.put(key, next);
        log.warn("event=copy_runtime_state.uncertain category=runtime_state reasonAlias=copy_state_uncertain friendlyReason=estado_de_copia_incierto explanation=hubo_orden_binance_o_estado_parcial_y_se_requiere_reconciliacion copyImpact=blocks_duplicate_open traceId={} originId={} userId={} allocationId={} strategy={} wallet={} symbol={} typeOperation={} reasonCode={} runtimeSize={}",
                safeLog(next.traceId()), safeLog(operation.getIdOrderOrigin()), safeLog(operation.getIdUser()), safeLog(operation.getIdWalletOrigin()),
                operation.getUserCopyAllocationId(), safeLog(operation.getCopyStrategyCode()),
                safeLog(operation.getParsymbol()), safeLog(operation.getTypeOperation()), safeLog(reasonCode), byRuntimeKey.size());
    }

    @Override
    public void forgetPending(String originId, String userId, String traceId, String reasonCode) {
        int removed = 0;
        for (java.util.Map.Entry<String, RuntimeCopyRef> entry : new ArrayList<>(byRuntimeKey.entrySet())) {
            RuntimeCopyRef current = entry.getValue();
            if (current == null || current.status() == RuntimeCopyStatus.ACTIVE || !matchesOriginUser(current, originId, userId)) {
                continue;
            }
            if (byRuntimeKey.remove(entry.getKey(), current)) removed++;
        }
        log.info("event=copy_runtime_state.pending_removed category=runtime_state reasonAlias=pending_cancelled friendlyReason=apertura_pendiente_cancelada explanation=se_remueve_el_estado_temporal_porque_no_quedo_copia_activa copyImpact=allows_future_open traceId={} originId={} userId={} reasonCode={} removed={} runtimeSize={}",
                safeLog(traceId), safeLog(originId), safeLog(userId), safeLog(reasonCode), removed, byRuntimeKey.size());
    }

    @Override
    public void markClosed(String originId, String userId) {
        int removed = 0;
        String traceId = CopyTraceIdUtil.copyTraceId(originId, userId, null, null);
        for (java.util.Map.Entry<String, RuntimeCopyRef> entry : new ArrayList<>(byRuntimeKey.entrySet())) {
            RuntimeCopyRef ref = entry.getValue();
            if (ref == null || !matchesOriginUser(ref, originId, userId)) {
                continue;
            }
            traceId = firstNonBlank(ref.traceId(), traceId);
            if (byRuntimeKey.remove(entry.getKey(), ref)) removed++;
        }
        log.info("event=copy_runtime_state.closed category=runtime_state reasonAlias=copy_closed friendlyReason=copia_cerrada_en_ram explanation=la_copia_se_removio_de_la_ruta_caliente copyImpact=no_active_copy traceId={} originId={} userId={} removed={} runtimeSize={}",
                safeLog(traceId), safeLog(originId), safeLog(userId), removed, byRuntimeKey.size());
    }

    @Override
    public void markClosed(CopyOperationDto operation) {
        if (operation == null) {
            return;
        }
        String key = key(operation);
        RuntimeCopyRef removed = key == null ? null : byRuntimeKey.remove(key);
        log.info("event=copy_runtime_state.closed category=runtime_state reasonAlias=copy_closed friendlyReason=copia_cerrada_en_ram explanation=la_copia_se_removio_de_la_ruta_caliente copyImpact=no_active_copy traceId={} originId={} userId={} allocationId={} strategy={} removed={} runtimeSize={}",
                removed == null ? traceId(operation.getIdOrderOrigin(), operation.getIdUser(), operation.getIdWalletOrigin(), operation.getParsymbol(), operation.getUserCopyAllocationId(), operation.getCopyStrategyCode()) : safeLog(removed.traceId()),
                safeLog(operation.getIdOrderOrigin()), safeLog(operation.getIdUser()), operation.getUserCopyAllocationId(),
                safeLog(operation.getCopyStrategyCode()), removed != null, byRuntimeKey.size());
    }

    @Override
    public int activeSize() {
        return (int) byRuntimeKey.values().stream()
                .filter(ref -> ref.status() == RuntimeCopyStatus.ACTIVE)
                .count();
    }

    private void refreshFromDatabase(String trigger) {
        long startedNs = System.nanoTime();
        try {
            List<CopyOperationEntity> active = repository.findAllByActiveTrue();
            ConcurrentMap<String, RuntimeCopyRef> next = new ConcurrentHashMap<>();
            Set<String> duplicatedKeys = new HashSet<>();
            for (CopyOperationEntity entity : active) {
                if (entity == null) {
                    continue;
                }
                String key = key(entity.getIdOrderOrigin(), entity.getIdUser(), entity.getUserCopyAllocationId(), entity.getCopyStrategyCode(), entity.getParsymbol(), entity.getTypeOperation());
                if (key == null) {
                    continue;
                }
                CopyOperationDto dto = mapper.toDto(entity);
                RuntimeCopyRef previous = next.put(key, RuntimeCopyRef.active(dto, traceId(dto.getIdOrderOrigin(), dto.getIdUser(), dto.getIdWalletOrigin(), dto.getParsymbol(), dto.getUserCopyAllocationId(), dto.getCopyStrategyCode())));
                if (previous != null) {
                    duplicatedKeys.add(key);
                }
            }

            List<RuntimeCopyRef> preserved = preserveFreshNonActive(next.keySet());
            for (RuntimeCopyRef ref : preserved) {
                String key = key(ref.originId(), ref.userId(), ref.allocationId(), ref.strategyCode(), ref.symbol(), ref.side());
                if (key != null) {
                    next.putIfAbsent(key, ref);
                }
            }

            Set<String> previousActiveKeys = byRuntimeKey.entrySet().stream()
                    .filter(e -> e.getValue().status() == RuntimeCopyStatus.ACTIVE)
                    .map(java.util.Map.Entry::getKey)
                    .collect(Collectors.toSet());
            Set<String> nextActiveKeys = next.entrySet().stream()
                    .filter(e -> e.getValue().status() == RuntimeCopyStatus.ACTIVE)
                    .map(java.util.Map.Entry::getKey)
                    .collect(Collectors.toSet());
            int missingInCache = countMissing(nextActiveKeys, previousActiveKeys);
            int missingInDb = countMissing(previousActiveKeys, nextActiveKeys);

            byRuntimeKey.clear();
            byRuntimeKey.putAll(next);
            log.info("event=copy_state.reconcile.ok category=runtime_state trigger={} dbActive={} runtimeActive={} runtimeTotal={} missingInCache={} missingInDb={} duplicateKeys={} preservedPending={} elapsedMs={}",
                    trigger, active.size(), activeSize(), byRuntimeKey.size(), missingInCache, missingInDb, duplicatedKeys.size(), preserved.size(), elapsedMs(startedNs));
        } catch (DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            log.warn("event=copy_state.reconcile.failed category=runtime_state trigger={} runtimeActive={} runtimeTotal={} errClass={} errMsg=\"{}\" elapsedMs={}",
                    trigger, activeSize(), byRuntimeKey.size(), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
        }
    }

    private List<RuntimeCopyRef> preserveFreshNonActive(Set<String> dbActiveKeys) {
        if (byRuntimeKey.isEmpty()) {
            return List.of();
        }
        List<RuntimeCopyRef> preserved = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (java.util.Map.Entry<String, RuntimeCopyRef> entry : byRuntimeKey.entrySet()) {
            RuntimeCopyRef ref = entry.getValue();
            if (ref == null || ref.status() == RuntimeCopyStatus.ACTIVE || dbActiveKeys.contains(entry.getKey())) {
                continue;
            }
            if (now - ref.updatedAtMs() <= pendingTtlMs) {
                preserved.add(ref);
            }
        }
        return preserved;
    }

    private int countMissing(Set<String> expected, Set<String> actual) {
        int count = 0;
        for (String key : expected) {
            if (!actual.contains(key)) {
                count++;
            }
        }
        return count;
    }

    private String key(CopyOperationDto operation) {
        if (operation == null) {
            return null;
        }
        return key(
                operation.getIdOrderOrigin(),
                operation.getIdUser(),
                operation.getUserCopyAllocationId(),
                operation.getCopyStrategyCode(),
                operation.getParsymbol(),
                operation.getTypeOperation()
        );
    }

    private String key(String originId, String userId, Long allocationId, String strategyCode, String symbol, String side) {
        String origin = normalize(originId);
        String user = normalize(userId);
        if (origin == null || user == null) {
            return null;
        }
        String allocationScope = allocationScope(allocationId, strategyCode);
        String symbolKey = CopySymbolIdentity.primaryBaseAsset(symbol);
        if (symbolKey == null) {
            symbolKey = normalize(symbol);
        }
        String sideKey = normalize(side);
        return origin + '|' + user + '|' + allocationScope + '|' + nullToAny(symbolKey) + '|' + nullToAny(sideKey);
    }

    private boolean matchesOriginUser(RuntimeCopyRef ref, String originId, String userId) {
        if (ref == null) {
            return false;
        }
        return Objects.equals(normalize(ref.originId()), normalize(originId))
                && Objects.equals(normalize(ref.userId()), normalize(userId));
    }

    private boolean sameAllocationScope(RuntimeCopyRef ref, Long allocationId, String strategyCode) {
        if (ref == null) {
            return false;
        }
        return Objects.equals(allocationScope(ref.allocationId(), ref.strategyCode()), allocationScope(allocationId, strategyCode));
    }

    private String allocationScope(Long allocationId, String strategyCode) {
        if (allocationId != null) {
            return "allocation:" + allocationId;
        }
        String strategy = normalize(strategyCode);
        return strategy == null ? "legacy" : "strategy:" + strategy;
    }

    private String nullToAny(String value) {
        return value == null ? "*" : value;
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        return second;
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

    private enum RuntimeCopyStatus {
        ACTIVE,
        PENDING_OPEN,
        UNCERTAIN
    }

    private record RuntimeCopyRef(
            String originId,
            String userId,
            String wallet,
            String symbol,
            String side,
            Long allocationId,
            String strategyCode,
            CopyOperationDto operation,
            RuntimeCopyStatus status,
            String traceId,
            long updatedAtMs
    ) {
        static RuntimeCopyRef active(CopyOperationDto dto, String traceId) {
            return new RuntimeCopyRef(
                    dto.getIdOrderOrigin(),
                    dto.getIdUser(),
                    dto.getIdWalletOrigin(),
                    dto.getParsymbol(),
                    dto.getTypeOperation(),
                    dto.getUserCopyAllocationId(),
                    dto.getCopyStrategyCode(),
                    dto,
                    RuntimeCopyStatus.ACTIVE,
                    traceId,
                    System.currentTimeMillis()
            );
        }

        static RuntimeCopyRef pending(String originId, String userId, String wallet, String symbol, String side, Long allocationId, String strategyCode, String traceId) {
            return new RuntimeCopyRef(originId, userId, wallet, symbol, side, allocationId, strategyCode, null, RuntimeCopyStatus.PENDING_OPEN, traceId, System.currentTimeMillis());
        }

        static RuntimeCopyRef uncertain(CopyOperationDto dto, String traceId) {
            return new RuntimeCopyRef(
                    dto.getIdOrderOrigin(),
                    dto.getIdUser(),
                    dto.getIdWalletOrigin(),
                    dto.getParsymbol(),
                    dto.getTypeOperation(),
                    dto.getUserCopyAllocationId(),
                    dto.getCopyStrategyCode(),
                    dto,
                    RuntimeCopyStatus.UNCERTAIN,
                    traceId,
                    System.currentTimeMillis()
            );
        }

        boolean isExpiredNonActive(long pendingTtlMs) {
            return status != RuntimeCopyStatus.ACTIVE && System.currentTimeMillis() - updatedAtMs > pendingTtlMs;
        }
    }
}
