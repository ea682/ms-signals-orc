package com.apunto.engine.service.copy.certification;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.client.BinanceFuturesPositionClientDto;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.repository.CopyOperationRepository;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserWalletCopyPreferenceRepository;
import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AutomaticLiveAdoptionServiceTest {

    private static final UUID USER_ID = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID CERT_ID = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    @Test
    void flatObservedAccountCreatesValidAdoptionAndActivatesPendingLive() {
        Fixture fixture = fixture(List.of());

        LiveAllocationActivationResult result = fixture.service.reconcile(allocation());

        assertTrue(result.activated(), result.reasonCode());
        assertTrue(fixture.adoptionStore.decision.valid());
        assertEquals(1, fixture.activationStore.activations);
    }

    @Test
    void untrackedBinancePositionIsAuditedAndBlocksActivation() {
        BinanceFuturesPositionClientDto manual = new BinanceFuturesPositionClientDto();
        manual.setSymbol("BTCUSDC");
        manual.setPositionAmt("0.01");
        manual.setMarginType("cross");
        Fixture fixture = fixture(List.of(manual));

        LiveAllocationActivationResult result = fixture.service.reconcile(allocation());

        assertFalse(result.activated());
        assertFalse(fixture.adoptionStore.decision.valid());
        assertTrue(fixture.adoptionStore.decision.reasonCodes().contains(
                "LIVE_ADOPTION_MANUAL_POSITIONS_CONFLICT"));
        assertEquals(0, fixture.activationStore.activations);
    }

    private static Fixture fixture(List<BinanceFuturesPositionClientDto> positions) {
        DetailUserEntity detail = new DetailUserEntity();
        UserEntity user = new UserEntity();
        user.setId(USER_ID);
        detail.setUser(user);
        detail.setUserActive(true);
        detail.setApiKeyBinar(true);
        detail.setAutoFollowCertifiedLive(true);
        detail.setCapital(1_000);
        detail.setMaxWallet(4);
        detail.setCapitalAsset("USDC");
        UserApiKeyEntity key = new UserApiKeyEntity();
        key.setId(UUID.randomUUID());
        key.setAccountPurpose(ExecutionAccountPurpose.LIVE);
        key.setActive(true);
        key.setApiKey("key");
        key.setApiSecret("secret");

        FuturesAssetBalanceClientResponse balance = FuturesAssetBalanceClientResponse.builder()
                .asset("USDC").marginBalance("500").build();
        BinanceClient binance = proxy(BinanceClient.class, (proxy, method, args) -> switch (method.getName()) {
            case "assetBalance" -> ApiResponse.<FuturesAssetBalanceClientResponse>builder()
                    .statusCode(200).data(balance).build();
            case "positions" -> ApiResponse.<List<BinanceFuturesPositionClientDto>>builder()
                    .statusCode(200).data(positions).build();
            default -> defaultValue(method.getReturnType());
        });
        DetailUserRepository details = proxy(DetailUserRepository.class, (proxy, method, args) ->
                "findByUser_Id".equals(method.getName()) ? detail : defaultValue(method.getReturnType()));
        UserApiKeyRepository keys = proxy(UserApiKeyRepository.class, (proxy, method, args) ->
                "findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue".equals(method.getName())
                        ? Optional.of(key) : defaultValue(method.getReturnType()));
        UserWalletCopyPreferenceRepository preferences = proxy(UserWalletCopyPreferenceRepository.class,
                (proxy, method, args) -> "isBlocked".equals(method.getName())
                        ? false : defaultValue(method.getReturnType()));
        CopyOperationRepository copies = proxy(CopyOperationRepository.class, (proxy, method, args) ->
                "findAllByIdUserAndActiveTrue".equals(method.getName())
                        ? List.of() : defaultValue(method.getReturnType()));

        LiveCertificationIdentity identity = new LiveCertificationIdentity(
                "0xabc", "MOVEMENT_ALL", "v1", "ALL", "ALL", BigDecimal.ZERO,
                new BigDecimal("1000000"), new BigDecimal("5"), "BINANCE", "USDC",
                "s1", "m1", "fee1", "fund1", "slip1", "liq1");
        LiveCertificationCatalogStore catalog = proxy(LiveCertificationCatalogStore.class,
                (proxy, method, args) -> "findIdentityById".equals(method.getName())
                        ? Optional.of(identity) : defaultValue(method.getReturnType()));
        CapturingAdoptionStore adoptionStore = new CapturingAdoptionStore();
        LiveUserAdoptionApplicationService adoption = new LiveUserAdoptionApplicationService(
                catalog, new LiveUserAdoptionPersistenceService(new LiveUserAdoptionValidator(), adoptionStore));
        CapturingActivationStore activationStore = new CapturingActivationStore();
        AutomaticLiveAdoptionService service = new AutomaticLiveAdoptionService(
                binance, details, keys, preferences, copies, adoption,
                new ManualLiveAllocationActivationService(activationStore), "CROSSED", Duration.ofHours(24));
        return new Fixture(service, adoptionStore, activationStore);
    }

    private static UserCopyAllocationEntity allocation() {
        OffsetDateTime now = OffsetDateTime.now();
        return UserCopyAllocationEntity.builder()
                .id(77L).idUser(USER_ID).walletId("0xabc").copyStrategyCode("MOVEMENT_ALL")
                .scopeType("ALL").scopeValue("ALL").executionMode("LIVE")
                .status(UserCopyAllocationEntity.Status.PAUSED).isActive(true)
                .allocationPct(new BigDecimal("0.1"))
                .allocationPctSource("SIGNALS_CURRENT_LIVE_DISTRIBUTION")
                .allocationPctSourceId(UUID.randomUUID())
                .allocationPctCalculatedAt(now.minusMinutes(1))
                .allocationPctValidUntil(now.plusMinutes(5))
                .walletTotalAllocationPct(new BigDecimal("0.1"))
                .leverageOverride(new BigDecimal("5"))
                .capitalAsset("USDC").resolvedQuoteAsset("USDC")
                .liveCertificationId(CERT_ID).activationAt(now).updatedAt(now).build();
    }

    private static final class CapturingAdoptionStore implements LiveUserAdoptionStore {
        private UserAdoptionValidationDecision decision;

        @Override
        public void upsert(UserAdoptionValidationRequest request, UserAdoptionValidationDecision decision) {
            this.decision = decision;
        }
    }

    private static final class CapturingActivationStore implements LiveAllocationActivationStore {
        private int activations;

        @Override
        public Optional<LiveAllocationActivationSnapshot> lockAllocation(Long allocationId) {
            return Optional.of(new LiveAllocationActivationSnapshot(
                    allocationId, USER_ID, "0xabc", "MOVEMENT_ALL", "ALL", "ALL",
                    "LIVE", "PAUSED", true, null));
        }

        @Override public Optional<LiveAllocationActivationAudit> findAudit(String key) { return Optional.empty(); }
        @Override public long countOpenOperations(Long allocationId) { return 0; }
        @Override public long countNonTerminalIntents(Long allocationId) { return 0; }

        @Override
        public Optional<LiveActivationAuthorization> findAuthorization(Long allocationId, UUID certificationId) {
            OffsetDateTime now = OffsetDateTime.now();
            return Optional.of(new LiveActivationAuthorization(
                    certificationId, LiveCertificationStatus.LIVE_APPROVED, "VALID", true,
                    now.minusSeconds(1), now.plusMinutes(30)));
        }

        @Override public boolean activate(Long id, String actor, String reason, OffsetDateTime at) { return false; }

        @Override
        public boolean activatePendingLive(Long id, String actor, String reason, OffsetDateTime at) {
            activations++;
            return true;
        }

        @Override public void appendAudit(LiveAllocationActivationAudit audit) { }
    }

    private record Fixture(AutomaticLiveAdoptionService service,
                           CapturingAdoptionStore adoptionStore,
                           CapturingActivationStore activationStore) { }

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
        if (Map.class.isAssignableFrom(type)) return Map.of();
        return null;
    }
}
