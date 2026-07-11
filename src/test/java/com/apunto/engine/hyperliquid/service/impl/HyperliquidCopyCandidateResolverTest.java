package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.copy.CopyRuntimeGuardPolicy;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import com.apunto.engine.service.copy.CopyStrategyGuardRuntimeCache;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.shared.enums.PositionSide;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperliquidCopyCandidateResolverTest {

    private static final String WALLET = "0xa445a0a15b1d50fa0c4bfe6796d9447e0da5329d";

    @Test
    void activeMicroLiveAllocationIsEligibleForOpenEvenWhenMetricGuardIsShadowOnly() {
        UUID userId = UUID.randomUUID();
        UserCopyAllocationEntity allocation = allocation(505L, userId, "MICRO_LIVE", "MOVEMENT_ALL");
        allocation.setLinkedShadowAllocationId(18L);
        allocation.setPromotedFromShadowAt(OffsetDateTime.now().minusMinutes(5));

        HyperliquidCopyCandidateResolver.CandidateUsers result = resolver(
                List.of(user(userId)),
                List.of(allocation),
                CopyStrategyGuardDecision.blocked("METRIC_COPY_GUARD_PAUSE_OPEN", "status=SHADOW_ONLY")
        ).resolve(mapped(PositionSide.LONG, "OPEN", "SOLUSDT"), CopyJobAction.OPEN);

        assertEquals(1, result.eligibleUsers().size());
        assertEquals(userId, result.eligibleUsers().get(0).getUser().getId());
    }

    @Test
    void livePromotedAllocationIsEligibleEvenWhenMetricGuardIsShadowOnly() {
        UUID userId = UUID.randomUUID();
        UserCopyAllocationEntity allocation = allocation(777L, userId, "LIVE", "MOVEMENT_ALL");
        allocation.setLinkedShadowAllocationId(505L);
        allocation.setPromotedFromShadowAt(OffsetDateTime.now().minusMinutes(5));

        HyperliquidCopyCandidateResolver.CandidateUsers result = resolver(
                List.of(user(userId)),
                List.of(allocation),
                CopyStrategyGuardDecision.shadowOnly("SUMMARY_NOT_FINAL_LIVE_BLOCKED", "status=SHADOW_ONLY", 0.0)
        ).resolve(mapped(PositionSide.LONG, "OPEN", "SOLUSDT"), CopyJobAction.OPEN);

        assertEquals(1, result.eligibleUsers().size());
        assertEquals(userId, result.eligibleUsers().get(0).getUser().getId());
    }

    @Test
    void directLiveWithoutPromotionIsBlockedByShadowOnly() {
        UUID userId = UUID.randomUUID();
        UserCopyAllocationEntity allocation = allocation(778L, userId, "LIVE", "MOVEMENT_ALL");

        HyperliquidCopyCandidateResolver.CandidateUsers result = resolver(
                List.of(user(userId)),
                List.of(allocation),
                CopyStrategyGuardDecision.shadowOnly("SUMMARY_NOT_FINAL_LIVE_BLOCKED", "status=SHADOW_ONLY", 0.0)
        ).resolve(mapped(PositionSide.LONG, "OPEN", "SOLUSDT"), CopyJobAction.OPEN);

        assertTrue(result.eligibleUsers().isEmpty());
        assertEquals("SUMMARY_NOT_FINAL_LIVE_BLOCKED", result.reasonCode());
    }

    @Test
    void shortOnlyDoesNotMatchLong() {
        UUID userId = UUID.randomUUID();
        UserCopyAllocationEntity allocation = allocation(506L, userId, "MICRO_LIVE", "SHORT_ONLY");
        allocation.setLinkedShadowAllocationId(22L);
        allocation.setPromotedFromShadowAt(OffsetDateTime.now().minusMinutes(5));

        HyperliquidCopyCandidateResolver.CandidateUsers result = resolver(
                List.of(user(userId)),
                List.of(allocation),
                CopyStrategyGuardDecision.allow()
        ).resolve(mapped(PositionSide.LONG, "OPEN", "SOLUSDT"), CopyJobAction.OPEN);

        assertTrue(result.eligibleUsers().isEmpty());
    }

    @Test
    void shortOnlyMatchesShort() {
        UUID userId = UUID.randomUUID();
        UserCopyAllocationEntity allocation = allocation(506L, userId, "MICRO_LIVE", "SHORT_ONLY");
        allocation.setLinkedShadowAllocationId(22L);
        allocation.setPromotedFromShadowAt(OffsetDateTime.now().minusMinutes(5));

        HyperliquidCopyCandidateResolver.CandidateUsers result = resolver(
                List.of(user(userId)),
                List.of(allocation),
                CopyStrategyGuardDecision.allow()
        ).resolve(mapped(PositionSide.SHORT, "OPEN", "SOLUSDT"), CopyJobAction.OPEN);

        assertEquals(1, result.eligibleUsers().size());
        assertEquals(userId, result.eligibleUsers().get(0).getUser().getId());
    }

    private static HyperliquidCopyCandidateResolver resolver(
            List<UserDetailDto> users,
            List<UserCopyAllocationEntity> allocations,
            CopyStrategyGuardDecision guardDecision
    ) {
        return new HyperliquidCopyCandidateResolver(
                new FakeUserDetailCachedService(users),
                new FakeAllocationService(allocations),
                new FakeActiveCopyOperationCache(),
                new CopyStrategyRuntimeRouter(),
                new FakeGuardRuntimeCache(guardDecision),
                new CopyRuntimeGuardPolicy(),
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        );
    }

    private static HyperliquidMappedDelta mapped(PositionSide side, String deltaType, String symbol) {
        OperacionDto operation = OperacionDto.builder()
                .idOperacion(UUID.randomUUID())
                .idCuenta(WALLET)
                .parSymbol(symbol)
                .tipoOperacion(side)
                .size(BigDecimal.ONE)
                .sizeQty(BigDecimal.ONE)
                .notionalUsd(new BigDecimal("100"))
                .marginUsedUsd(new BigDecimal("20"))
                .leverage(new BigDecimal("5"))
                .precioEntrada(new BigDecimal("77.513"))
                .operacionActiva(true)
                .build();
        OperacionEvent event = new OperacionEvent(OperacionEvent.Tipo.ABIERTA, operation, deltaType);
        return new HyperliquidMappedDelta("idem", "pos", WALLET, symbol, side.name(), deltaType, event, null);
    }

    private static UserCopyAllocationEntity allocation(Long id, UUID userId, String mode, String strategy) {
        return UserCopyAllocationEntity.builder()
                .id(id)
                .idUser(userId)
                .walletId(WALLET)
                .copyStrategyCode(strategy)
                .scopeType("strategy")
                .scopeValue(strategy)
                .allocationPct(new BigDecimal("0.000001"))
                .executionMode(mode)
                .isActive(true)
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .build();
    }

    private static UserDetailDto user(UUID id) {
        UserEntity user = new UserEntity();
        user.setId(id);
        user.setEmail(id + "@example.test");
        DetailUserEntity detail = new DetailUserEntity();
        detail.setUser(user);
        detail.setUserActive(true);
        detail.setApiKeyBinar(true);
        detail.setCapital(1000);
        detail.setCapitalAsset("USDC");
        detail.setLeverage(5);
        return new UserDetailDto(user, detail, new UserApiKeyEntity());
    }

    private record FakeUserDetailCachedService(List<UserDetailDto> users) implements UserDetailCachedService {
        @Override
        public List<UserDetailDto> getUsers() {
            return users;
        }

        @Override
        public List<UserDetailDto> getUsersCachedOnly() {
            return users;
        }

        @Override
        public Optional<UserDetailDto> getUserById(String userId) {
            return users.stream()
                    .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                    .filter(u -> u.getUser().getId().toString().equals(userId))
                    .findFirst();
        }

        @Override
        public void updateRuntimeCapital(UUID userId, Integer capital, String capitalAsset) {
        }
    }

    private record FakeGuardRuntimeCache(CopyStrategyGuardDecision guardDecision) implements CopyStrategyGuardRuntimeCache {
        @Override
        public CopyStrategyGuardDecision evaluateCached(String walletId, String strategyCode) {
            return guardDecision == null ? CopyStrategyGuardDecision.allow() : guardDecision;
        }
    }

    private static final class FakeAllocationService implements UserCopyAllocationService {
        private final List<UserCopyAllocationEntity> allocations;

        private FakeAllocationService(List<UserCopyAllocationEntity> allocations) {
            this.allocations = allocations;
        }

        @Override
        public void syncDistribution(List<MetricaWalletDto> candidates) {
        }

        @Override
        public List<UserCopyAllocationEntity> getWalletUserId(UUID idUser) {
            return allocations.stream().filter(a -> a.getIdUser().equals(idUser)).toList();
        }

        @Override
        public Set<UUID> getActiveUserIdsByWallet(String walletId) {
            return allocations.stream().map(UserCopyAllocationEntity::getIdUser).collect(java.util.stream.Collectors.toSet());
        }

        @Override
        public List<UserCopyAllocationEntity> getActiveAllocationsByWallet(String walletId) {
            return allocations.stream()
                    .filter(a -> a.getWalletId().equalsIgnoreCase(walletId))
                    .toList();
        }

        @Override
        public List<UserCopyAllocationEntity> getActiveAllocationsByWalletCachedOnly(String walletId) {
            return getActiveAllocationsByWallet(walletId);
        }

        @Override
        public List<UserCopyAllocationEntity> getActiveAllocationsForUserWallet(UUID idUser, String walletId) {
            return allocations.stream()
                    .filter(a -> a.getIdUser().equals(idUser))
                    .filter(a -> a.getWalletId().equalsIgnoreCase(walletId))
                    .toList();
        }

        @Override
        public Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId) {
            return getActiveAllocationsForUserWallet(idUser, walletId).stream().findFirst();
        }

        @Override
        public Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId, String strategyCode) {
            return getActiveAllocationsForUserWallet(idUser, walletId).stream()
                    .filter(a -> a.getCopyStrategyCode().equals(strategyCode))
                    .findFirst();
        }

        @Override
        public Optional<UserCopyAllocationEntity> findOpenAllocation(UUID idUser, String walletId, String strategyCode) {
            return findActiveAllocation(idUser, walletId, strategyCode);
        }

        @Override
        public void markGuardBlocked(UUID idUser, String walletId, String strategyCode, String targetStatus, String reason, OffsetDateTime cooldownUntil) {
            throw new AssertionError("runtime guard must not mutate allocation status in hot path");
        }
    }

    private static final class FakeActiveCopyOperationCache implements ActiveCopyOperationCache {
        @Override public boolean isActive(String originId, String userId) { return false; }
        @Override public boolean isActive(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation) { return false; }
        @Override public boolean isKnown(String originId, String userId) { return false; }
        @Override public boolean isKnown(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation) { return false; }
        @Override public CopyOperationDto activeOperation(String originId, String userId) { return null; }
        @Override public CopyOperationDto activeOperation(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation) { return null; }
        @Override public List<CopyOperationDto> activeOperations(String originId, String userId) { return List.of(); }
        @Override public List<CopyOperationDto> activeOperationsByUserAndWallet(String userId, String walletId) { return List.of(); }
        @Override public List<CopyOperationDto> activeOperationsByUser(String userId) { return List.of(); }
        @Override public Set<String> activeUserIds(String originId) { return Set.of(); }
        @Override public Set<String> activeUserIdsByWallet(String walletId) { return Set.of(); }
        @Override public Set<String> activeUserIdsByWalletAndSymbol(String walletId, String symbol) { return Set.of(); }
        @Override public Set<String> activeUserIdsByWalletAndBaseSymbol(String walletId, String symbol) { return Set.of(); }
        @Override public String traceId(String originId, String userId, String walletId, String symbol) { return "trace"; }
        @Override public String traceId(String originId, String userId, String walletId, String symbol, Long allocationId, String strategyCode) { return "trace"; }
        @Override public void markPendingOpen(String originId, String userId, String walletId, String symbol, String typeOperation, String traceId) {}
        @Override public void markPendingOpen(String originId, String userId, String walletId, String symbol, String typeOperation, Long allocationId, String strategyCode, String traceId) {}
        @Override public void markOpen(CopyOperationDto operation) {}
        @Override public void markUncertain(CopyOperationDto operation, String traceId, String reasonCode) {}
        @Override public void forgetPending(String originId, String userId, String traceId, String reasonCode) {}
        @Override public void markClosed(String originId, String userId) {}
        @Override public void markClosed(CopyOperationDto operation) {}
        @Override public int activeSize() { return 0; }
    }
}
