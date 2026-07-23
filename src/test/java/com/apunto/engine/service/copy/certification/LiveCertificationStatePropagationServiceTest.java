package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.repository.UserWalletCopyPreferenceRepository;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolution;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolver;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class LiveCertificationStatePropagationServiceTest {

    private static final UUID CERTIFICATION_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID USER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Test
    void liveApprovalProvisionsPausedLiveForEligibleAutoFollowerWithoutMicroLive() {
        Fixture fixture = fixture(false);

        fixture.service.propagate(CERTIFICATION_ID, LiveCertificationStatus.LIVE_APPROVED, "approved");

        UserCopyAllocationEntity saved = fixture.saved.get();
        assertNotNull(saved);
        assertEquals(USER_ID, saved.getIdUser());
        assertEquals("LIVE", saved.getExecutionMode());
        assertEquals(UserCopyAllocationEntity.Status.PAUSED, saved.getStatus());
        assertEquals("LIVE_ADOPTION_VALIDATION_REQUIRED", saved.getStatusReason());
        assertEquals(CERTIFICATION_ID, saved.getLiveCertificationId());
        assertEquals(new BigDecimal("0.120000"), saved.getAllocationPct());
        assertNotNull(saved.getActivationAt());
    }

    @Test
    void liveApprovalLeavesCurrentMicroParticipantForAtomicMicroPromotion() {
        Fixture fixture = fixture(true);

        fixture.service.propagate(CERTIFICATION_ID, LiveCertificationStatus.LIVE_APPROVED, "approved");

        assertEquals(null, fixture.saved.get());
    }

    @Test
    void degradationNeverCreatesMicroAllocationDirectlyBeforeDurableCapacityAdmission() {
        Fixture fixture = fixture(false);

        fixture.service.propagate(CERTIFICATION_ID, LiveCertificationStatus.LIVE_DEGRADED, "quality_degraded");

        assertNull(fixture.saved.get(), "recertification must pass through its durable priority queue");
        MicroLiveRecertificationRequest queued = fixture.queued.get();
        assertNotNull(queued);
        assertEquals(CERTIFICATION_ID, queued.certificationId());
        assertEquals(USER_ID, queued.userId());
        assertEquals("0xabc", queued.walletId());
        assertEquals("MICRO_LIVE_RECERTIFICATION_PENDING_CAPACITY", queued.reasonCode());
    }

    private static Fixture fixture(boolean openMicro) {
        DetailUserEntity detail = new DetailUserEntity();
        UserEntity user = new UserEntity();
        user.setId(USER_ID);
        detail.setUser(user);
        detail.setUserActive(true);
        detail.setApiKeyBinar(true);
        detail.setAutoFollowCertifiedLive(true);
        detail.setParticipateInMicroLive(true);
        detail.setCapital(1_000);
        detail.setMaxWallet(4);
        detail.setCapitalAsset("USDC");
        UserApiKeyEntity key = new UserApiKeyEntity();
        key.setId(UUID.randomUUID());
        key.setActive(true);
        key.setApiKey("key");
        key.setApiSecret("secret");

        LiveCertificationIdentity identity = new LiveCertificationIdentity(
                "0xabc", "MOVEMENT_ALL", "v1", "ALL", "ALL",
                BigDecimal.ZERO, new BigDecimal("1000000"), new BigDecimal("5"),
                "BINANCE", "USDC", "s1", "m1", "f1", "fund1", "slip1", "liq1");
        OffsetDateTime now = OffsetDateTime.now();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        AtomicReference<MicroLiveRecertificationRequest> queued = new AtomicReference<>();
        UserCopyAllocationRepository allocations = proxy(UserCopyAllocationRepository.class, (proxy, method, args) -> {
            return switch (method.getName()) {
                case "findAllByLiveCertificationIdAndExecutionMode" -> List.of();
                case "markCertificationAllocationsPendingRevalidation" -> 0;
                case "findOpenAllocationForUserWalletStrategyScopeAndMode" ->
                        "MICRO_LIVE".equals(args[5]) && openMicro
                                ? Optional.of(UserCopyAllocationEntity.builder().id(9L).build())
                                : Optional.empty();
                case "saveAndFlush" -> {
                    UserCopyAllocationEntity value = (UserCopyAllocationEntity) args[0];
                    value.setId(77L);
                    saved.set(value);
                    yield value;
                }
                default -> defaultValue(method.getReturnType());
            };
        });
        DetailUserRepository details = proxy(DetailUserRepository.class, (proxy, method, args) ->
                ("findEligibleAutoFollowCertifiedLiveUsers".equals(method.getName())
                        || "findEligibleMicroLiveUsers".equals(method.getName()))
                        ? List.of(detail) : defaultValue(method.getReturnType()));
        UserApiKeyRepository keys = proxy(UserApiKeyRepository.class, (proxy, method, args) ->
                "findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue".equals(method.getName())
                        ? Optional.of(key) : defaultValue(method.getReturnType()));
        UserWalletCopyPreferenceRepository preferences = proxy(UserWalletCopyPreferenceRepository.class,
                (proxy, method, args) -> "isBlocked".equals(method.getName())
                        ? false : defaultValue(method.getReturnType()));
        LiveCertificationCatalogStore catalog = proxy(LiveCertificationCatalogStore.class,
                (proxy, method, args) -> "findIdentityById".equals(method.getName())
                        ? Optional.of(identity) : defaultValue(method.getReturnType()));
        LiveAllocationPercentageResolver percentages = request -> LiveAllocationPercentageResolution.resolved(
                new BigDecimal("0.12"), new BigDecimal("0.12"), "SIGNALS_CURRENT_LIVE_DISTRIBUTION",
                UUID.randomUUID(), now, now.plusMinutes(5));
        MicroLiveRecertificationQueue queue = request -> {
            queued.set(request);
            return com.apunto.engine.entity.MicroLiveRecertificationRequestEntity.builder()
                    .id(UUID.randomUUID())
                    .certificationId(request.certificationId())
                    .userId(request.userId())
                    .executionAccountId(request.executionAccountId())
                    .walletId(request.walletId())
                    .strategyCode(request.strategyCode())
                    .strategyVersion(request.strategyVersion())
                    .build();
        };

        return new Fixture(saved, queued, new LiveCertificationStatePropagationService(
                allocations, details, keys, preferences, catalog, percentages,
                new LiveAllocationSafetyTransitionService(allocations), queue));
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class) return false;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (Optional.class.isAssignableFrom(type)) return Optional.empty();
        if (List.class.isAssignableFrom(type)) return List.of();
        return null;
    }

    private static final class Fixture {
        private final AtomicReference<UserCopyAllocationEntity> saved;
        private final AtomicReference<MicroLiveRecertificationRequest> queued;
        private final LiveCertificationStatePropagationService service;

        private Fixture(AtomicReference<UserCopyAllocationEntity> saved,
                        AtomicReference<MicroLiveRecertificationRequest> queued,
                        LiveCertificationStatePropagationService service) {
            this.saved = saved;
            this.queued = queued;
            this.service = service;
        }
    }
}
