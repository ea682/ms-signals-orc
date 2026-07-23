package com.apunto.engine.service.copy.account;

import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.UserApiKeyRepository;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionAccountResolverTest {

    private static final UUID USER_ID = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    @Test
    void eachModeSelectsItsExplicitPurposeRegardlessOfCredentialRecency() {
        UserApiKeyEntity live = account(ExecutionAccountPurpose.LIVE, true, "live-old");
        UserApiKeyEntity micro = account(ExecutionAccountPurpose.MICRO_LIVE, true, "micro-new");
        ExecutionAccountResolver resolver = resolver(List.of(live, micro));

        ExecutionAccountResolution microResolution = resolver.resolve(USER_ID, "MICRO_LIVE", null);
        ExecutionAccountResolution liveResolution = resolver.resolve(USER_ID, "LIVE", null);

        assertTrue(microResolution.allowed());
        assertSame(micro, microResolution.account());
        assertTrue(liveResolution.allowed());
        assertSame(live, liveResolution.account());
    }

    @Test
    void missingPurposeNeverFallsBackToTheOtherBinanceAccount() {
        UserApiKeyEntity live = account(ExecutionAccountPurpose.LIVE, true, "live");
        ExecutionAccountResolver resolver = resolver(List.of(live));

        ExecutionAccountResolution resolution = resolver.resolve(USER_ID, "MICRO_LIVE", null);

        assertFalse(resolution.allowed());
        assertEquals("MICRO_LIVE_EXECUTION_ACCOUNT_MISSING", resolution.reasonCode());
        assertEquals(null, resolution.account());
    }

    @Test
    void immutableBindingRejectsPurposeMismatchAndInactiveAccount() {
        UserApiKeyEntity live = account(ExecutionAccountPurpose.LIVE, true, "live");
        UserApiKeyEntity inactiveMicro = account(ExecutionAccountPurpose.MICRO_LIVE, false, "micro");
        ExecutionAccountResolver resolver = resolver(List.of(live, inactiveMicro));

        ExecutionAccountResolution mismatch = resolver.resolve(USER_ID, "MICRO_LIVE", live.getId());
        ExecutionAccountResolution inactive = resolver.resolve(USER_ID, "MICRO_LIVE", inactiveMicro.getId());

        assertFalse(mismatch.allowed());
        assertEquals("EXECUTION_ACCOUNT_PURPOSE_MISMATCH", mismatch.reasonCode());
        assertFalse(inactive.allowed());
        assertEquals("EXECUTION_ACCOUNT_INACTIVE", inactive.reasonCode());
    }

    @Test
    void credentialRotationKeepsStableExchangeAccountIdentity() {
        UserApiKeyEntity micro = account(ExecutionAccountPurpose.MICRO_LIVE, true, "before");
        ExecutionAccountResolver resolver = resolver(List.of(micro));
        UserCopyAllocationEntity allocation = UserCopyAllocationEntity.builder()
                .idUser(USER_ID).executionMode("MICRO_LIVE").exchangeAccountId(micro.getId()).build();

        micro.setApiKey("after");
        micro.setApiSecret("rotated-secret");
        ExecutionAccountResolution resolution = resolver.resolve(allocation);

        assertTrue(resolution.allowed());
        assertEquals(micro.getId(), resolution.exchangeAccountId());
        assertEquals("after", resolution.account().getApiKey());
    }

    @Test
    void sameBinanceCredentialRegisteredForLiveAndMicroIsRejectedAsNotIsolated() {
        UserApiKeyEntity live = account(ExecutionAccountPurpose.LIVE, true, "same-key");
        UserApiKeyEntity micro = account(ExecutionAccountPurpose.MICRO_LIVE, true, "same-key");
        ExecutionAccountResolver resolver = resolver(List.of(live, micro));

        ExecutionAccountResolution resolution = resolver.resolve(USER_ID, "MICRO_LIVE", null);

        assertFalse(resolution.allowed());
        assertEquals("EXECUTION_ACCOUNTS_NOT_ISOLATED", resolution.reasonCode());
    }

    @Test
    void distinctCredentialsResolvingToSameStableBinanceAccountAreRejected() {
        UserApiKeyEntity live = account(ExecutionAccountPurpose.LIVE, true, "live-key");
        live.setExchangeAccountRef("same-account-alias");
        UserApiKeyEntity micro = account(ExecutionAccountPurpose.MICRO_LIVE, true, "micro-key");
        micro.setExchangeAccountRef("SAME-ACCOUNT-ALIAS");
        ExecutionAccountResolver resolver = resolver(List.of(live, micro));

        ExecutionAccountResolution resolution = resolver.resolve(USER_ID, "LIVE", null);

        assertFalse(resolution.allowed());
        assertEquals("EXECUTION_ACCOUNTS_NOT_ISOLATED", resolution.reasonCode());
    }

    private static UserApiKeyEntity account(ExecutionAccountPurpose purpose, boolean active, String apiKey) {
        UserApiKeyEntity account = new UserApiKeyEntity();
        account.setId(UUID.randomUUID());
        account.setUser(user());
        account.setExchange("BINANCE");
        account.setAccountPurpose(purpose);
        account.setActive(active);
        account.setApiKey(apiKey);
        account.setApiSecret("secret");
        return account;
    }

    private static com.apunto.engine.entity.UserEntity user() {
        com.apunto.engine.entity.UserEntity user = new com.apunto.engine.entity.UserEntity();
        user.setId(USER_ID);
        return user;
    }

    @SuppressWarnings("unchecked")
    private static ExecutionAccountResolver resolver(List<UserApiKeyEntity> accounts) {
        List<UserApiKeyEntity> mutable = new ArrayList<>(accounts);
        UserApiKeyRepository repository = (UserApiKeyRepository) Proxy.newProxyInstance(
                UserApiKeyRepository.class.getClassLoader(), new Class<?>[]{UserApiKeyRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "findById" -> mutable.stream().filter(a -> a.getId().equals(args[0])).findFirst();
                    case "findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue" -> mutable.stream()
                            .filter(UserApiKeyEntity::isActive)
                            .filter(a -> a.getUser().getId().equals(args[0]))
                            .filter(a -> a.getExchange().equalsIgnoreCase((String) args[1]))
                            .filter(a -> a.getAccountPurpose() == args[2]).findFirst();
                    case "findByUser_IdAndExchangeIgnoreCaseAndAccountPurpose" -> mutable.stream()
                            .filter(a -> a.getUser().getId().equals(args[0]))
                            .filter(a -> a.getExchange().equalsIgnoreCase((String) args[1]))
                            .filter(a -> a.getAccountPurpose() == args[2]).findFirst();
                    default -> method.getReturnType() == Optional.class ? Optional.empty() : null;
                });
        return new ExecutionAccountResolver(repository);
    }
}
