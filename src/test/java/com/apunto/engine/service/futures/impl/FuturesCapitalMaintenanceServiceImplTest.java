package com.apunto.engine.service.futures.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientResponse;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.UserDetailService;
import com.apunto.engine.service.copy.budget.CopyBudgetResolver;
import com.apunto.engine.service.copy.budget.CopyBudgetResolver.CopyBudgetDecision;
import com.apunto.engine.service.copy.budget.CopyBudgetResolver.CopyBudgetRequest;
import com.apunto.engine.service.futures.BinanceFuturesWalletService;
import com.apunto.engine.service.futures.FuturesBnbConversionPolicy;
import com.apunto.engine.service.futures.FuturesBnbPriceService;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FuturesCapitalMaintenanceServiceImplTest {

    @Test
    void schedulerUpdatesUsdcCapitalFromAvailableBalance() {
        UserDetailDto user = activeUser(192, "USDC", true);
        CapturingWalletService wallet = new CapturingWalletService();
        CapturingDetailRepository repository = new CapturingDetailRepository();
        CapturingCache cache = new CapturingCache();
        FuturesCapitalMaintenanceServiceImpl service = service(List.of(user), wallet, repository, cache);

        service.maintainAllActiveUsersCapital();

        assertEquals(896, repository.lastCapital.get());
        assertEquals(user.getDetail().getId(), repository.lastDetailId.get());
        assertEquals(896, user.getDetail().getCapital());
        assertEquals(896, cache.lastCapital.get());
        assertEquals("USDC", cache.lastAsset.get());
        assertTrue(wallet.requestedAssets.contains("USDC"));
    }

    @Test
    void schedulerDoesNotKeepOldCapitalWhenBinanceRespondsOk() {
        UserDetailDto user = activeUser(192, "USDC", true);
        CapturingDetailRepository repository = new CapturingDetailRepository();

        service(List.of(user), new CapturingWalletService(), repository, new CapturingCache())
                .maintainAllActiveUsersCapital();

        assertFalse(Integer.valueOf(192).equals(repository.lastCapital.get()));
        assertEquals(896, repository.lastCapital.get());
    }

    @Test
    void schedulerSkipsInactiveApiUsers() {
        UserDetailDto user = activeUser(192, "USDC", false);
        CapturingWalletService wallet = new CapturingWalletService();
        CapturingDetailRepository repository = new CapturingDetailRepository();

        service(List.of(user), wallet, repository, new CapturingCache())
                .maintainAllActiveUsersCapital();

        assertTrue(wallet.requestedAssets.isEmpty());
        assertEquals(0, repository.updateCalls.get());
        assertEquals(192, user.getDetail().getCapital());
    }

    @Test
    void schedulerDoesNotOverwriteCapitalWithZeroWhenBinanceFails() {
        UserDetailDto user = activeUser(192, "USDC", true);
        CapturingWalletService wallet = new CapturingWalletService();
        wallet.fail = true;
        CapturingDetailRepository repository = new CapturingDetailRepository();

        service(List.of(user), wallet, repository, new CapturingCache())
                .maintainAllActiveUsersCapital();

        assertEquals(0, repository.updateCalls.get());
        assertEquals(192, user.getDetail().getCapital());
    }

    @Test
    void schedulerUsesConfiguredCapitalAsset() {
        UserDetailDto user = activeUser(192, "USDT", true);
        CapturingWalletService wallet = new CapturingWalletService();
        CapturingDetailRepository repository = new CapturingDetailRepository();

        service(List.of(user), wallet, repository, new CapturingCache())
                .maintainAllActiveUsersCapital();

        assertTrue(wallet.requestedAssets.contains("USDT"));
        assertEquals(321, repository.lastCapital.get());
    }

    @Test
    void schedulerSkipsInvalidCapitalAssetWithoutBreakingCycle() {
        UserDetailDto user = activeUser(192, "EUR", true);
        CapturingWalletService wallet = new CapturingWalletService();
        CapturingDetailRepository repository = new CapturingDetailRepository();

        service(List.of(user), wallet, repository, new CapturingCache())
                .maintainAllActiveUsersCapital();

        assertTrue(wallet.requestedAssets.isEmpty());
        assertEquals(0, repository.updateCalls.get());
        assertEquals(192, user.getDetail().getCapital());
    }

    @Test
    void schedulerCapitalChangeDoesNotAffectMicroLiveFixedBudget() {
        assertEquals(new BigDecimal("100.000000000000"), microBudgetForCapital(192).budgetUsd());
        assertEquals(new BigDecimal("100.000000000000"), microBudgetForCapital(896).budgetUsd());
        assertEquals(new BigDecimal("100.000000000000"), microBudgetForCapital(100_000).budgetUsd());
    }

    private static CopyBudgetDecision microBudgetForCapital(int capital) {
        return CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(BigDecimal.valueOf(capital))
                .allocationPct(new BigDecimal("0.99"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .capitalAsset("USDC")
                .build());
    }

    private static FuturesCapitalMaintenanceServiceImpl service(List<UserDetailDto> users,
                                                                CapturingWalletService wallet,
                                                                CapturingDetailRepository repository,
                                                                CapturingCache cache) {
        UserDetailService userDetailService = () -> users;
        FuturesBnbPriceService bnbPriceService = quoteAsset -> BigDecimal.ONE;
        return new FuturesCapitalMaintenanceServiceImpl(
                userDetailService,
                wallet,
                bnbPriceService,
                new FuturesBnbConversionPolicy(),
                repository.proxy(),
                cache
        );
    }

    private static UserDetailDto activeUser(int capital, String capitalAsset, boolean apiActive) {
        UserEntity user = new UserEntity();
        user.setId(UUID.randomUUID());
        user.setEmail("test@example.com");

        DetailUserEntity detail = new DetailUserEntity();
        detail.setId(UUID.randomUUID());
        detail.setUser(user);
        detail.setUserActive(true);
        detail.setApiKeyBinar(apiActive);
        detail.setCapital(capital);
        detail.setCapitalAsset(capitalAsset);

        UserApiKeyEntity apiKey = new UserApiKeyEntity();
        apiKey.setId(UUID.randomUUID());
        apiKey.setUser(user);
        apiKey.setExchange("BINANCE");
        apiKey.setApiKey("api-key");
        apiKey.setApiSecret("secret");

        return new UserDetailDto(user, detail, apiKey);
    }

    private static final class CapturingWalletService implements BinanceFuturesWalletService {
        private final List<String> requestedAssets = new ArrayList<>();
        private boolean fail;

        @Override
        public FuturesAssetBalanceClientResponse getAssetBalance(UserDetailDto userDetail, String asset) {
            requestedAssets.add(asset);
            if (fail) {
                throw new EngineException(ErrorCode.BINANCE_CLIENT_ERROR, "binance unavailable");
            }
            if ("BNB".equals(asset)) {
                return balance("BNB", "10", "10");
            }
            if ("USDT".equals(asset)) {
                return balance("USDT", "321.99990000", "321.99990000");
            }
            return balance("USDC", "896.65303430", "896.65303430");
        }

        @Override
        public FuturesConvertToBnbClientResponse convertStableAssetToBnb(UserDetailDto userDetail,
                                                                         FuturesCapitalAsset fromAsset,
                                                                         BigDecimal amount) {
            throw new AssertionError("No conversion should be attempted in these tests");
        }

        private FuturesAssetBalanceClientResponse balance(String asset, String walletBalance, String availableBalance) {
            return FuturesAssetBalanceClientResponse.builder()
                    .asset(asset)
                    .walletBalance(walletBalance)
                    .availableBalance(availableBalance)
                    .marginBalance(walletBalance)
                    .crossWalletBalance(walletBalance)
                    .build();
        }
    }

    private static final class CapturingDetailRepository {
        private final AtomicReference<UUID> lastDetailId = new AtomicReference<>();
        private final AtomicReference<Integer> lastCapital = new AtomicReference<>();
        private final AtomicInteger updateCalls = new AtomicInteger();

        private DetailUserRepository proxy() {
            return (DetailUserRepository) Proxy.newProxyInstance(
                    DetailUserRepository.class.getClassLoader(),
                    new Class<?>[]{DetailUserRepository.class},
                    (proxy, method, args) -> {
                        if ("updateCapitalById".equals(method.getName())) {
                            lastDetailId.set((UUID) args[0]);
                            lastCapital.set((Integer) args[1]);
                            updateCalls.incrementAndGet();
                            return 1;
                        }
                        if ("toString".equals(method.getName())) {
                            return "CapturingDetailRepository";
                        }
                        if ("hashCode".equals(method.getName())) {
                            return System.identityHashCode(proxy);
                        }
                        if ("equals".equals(method.getName())) {
                            return proxy == args[0];
                        }
                        Class<?> returnType = method.getReturnType();
                        if (returnType.equals(boolean.class)) {
                            return false;
                        }
                        if (returnType.equals(int.class) || returnType.equals(long.class) || returnType.equals(short.class)
                                || returnType.equals(byte.class) || returnType.equals(float.class) || returnType.equals(double.class)) {
                            return 0;
                        }
                        return null;
                    });
        }
    }

    private static final class CapturingCache implements UserDetailCachedService {
        private final AtomicReference<UUID> lastUserId = new AtomicReference<>();
        private final AtomicReference<Integer> lastCapital = new AtomicReference<>();
        private final AtomicReference<String> lastAsset = new AtomicReference<>();

        @Override
        public List<UserDetailDto> getUsers() {
            return List.of();
        }

        @Override
        public Optional<UserDetailDto> getUserById(String userId) {
            return Optional.empty();
        }

        @Override
        public void updateRuntimeCapital(UUID userId, Integer capital, String capitalAsset) {
            lastUserId.set(userId);
            lastCapital.set(capital);
            lastAsset.set(capitalAsset);
        }
    }
}
