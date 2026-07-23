package com.apunto.engine.service.copy.account;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.shared.dto.ApiResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroLiveCapacityGateTest {

    @Test
    void fifthReservationIsAllowedButSixthIsBlocked() {
        Fixture fixture = fixture("500", "500", 4);
        assertTrue(fixture.gate().evaluate(fixture.account(), UUID.randomUUID(), "USDC").allowed());

        fixture.occupied().set(5);
        MicroLiveCapacityDecision sixth = fixture.gate().evaluate(fixture.account(), UUID.randomUUID(), "USDC");
        assertFalse(sixth.allowed());
        assertEquals("MICRO_LIVE_CAPACITY_EXHAUSTED", sixth.reasonCode());
    }

    @Test
    void lossesReduceCapacityEvenWhenConfiguredSlotLimitHasNotBeenReached() {
        Fixture fixture = fixture("480", "480", 4);
        MicroLiveCapacityDecision result = fixture.gate().evaluate(
                fixture.account(), UUID.randomUUID(), "USDC");
        assertFalse(result.allowed());
        assertEquals("MICRO_LIVE_ACCOUNT_UNDER_RESERVED", result.reasonCode());
    }

    @Test
    void availableBalanceDoesNotReplaceAuthoritativeEquityForLogicalCapacity() {
        Fixture fixture = fixture("500", "90", 1);
        MicroLiveCapacityDecision result = fixture.gate().evaluate(
                fixture.account(), UUID.randomUUID(), "USDC");
        assertTrue(result.allowed());
        assertEquals(5, result.effectiveMaxSlots());
    }

    @ParameterizedTest
    @CsvSource({
            "499.99,4",
            "500,5",
            "999.99,9",
            "1000,10",
            "1250,12"
    })
    void equityBoundariesDetermineDynamicCapacity(String walletBalance, int expectedCapacity) {
        MicroLiveCapacityProperties properties = properties();
        properties.setMaxConcurrentAllocations(0);
        MicroLiveCapacityDecision result = gate(walletBalance, walletBalance, 0, properties)
                .evaluate(account(), UUID.randomUUID(), "USDC");

        assertTrue(result.allowed());
        assertEquals(expectedCapacity, result.effectiveMaxSlots());
    }

    @Test
    void configuredMaximumAndSafetyBufferCapTheoreticalCapacity() {
        MicroLiveCapacityProperties properties = properties();
        properties.setMaxConcurrentAllocations(7);
        properties.setSafetyBufferUsdc(new java.math.BigDecimal("200"));

        MicroLiveCapacityDecision result = gate("1000", "800", 0, properties)
                .evaluate(account(), UUID.randomUUID(), "USDC");

        assertTrue(result.allowed());
        assertEquals(7, result.effectiveMaxSlots());
    }

    @Test
    void reservedRecertificationSlotIsUnavailableToShadowButAvailableToRecertification() {
        MicroLiveCapacityProperties properties = properties();
        properties.setMaxConcurrentAllocations(0);
        properties.setReservedRecertificationSlots(1);
        MicroLiveCapacityGate gate = gate("500", "500", 4, properties);

        MicroLiveCapacityDecision shadow = gate.evaluate(
                account(), UUID.randomUUID(), "USDC", MicroLiveAdmissionPriority.SHADOW_PROMOTION);
        MicroLiveCapacityDecision recertification = gate.evaluate(
                account(), UUID.randomUUID(), "USDC", MicroLiveAdmissionPriority.RECERTIFICATION);

        assertFalse(shadow.allowed());
        assertEquals("MICRO_LIVE_CAPACITY_EXHAUSTED", shadow.reasonCode());
        assertTrue(recertification.allowed());
    }

    @Test
    void zeroConfiguredMaxUsesEquityWithoutAnArtificialFiveSlotCeiling() {
        AtomicLong occupiedSlots = new AtomicLong(5);
        UserCopyAllocationRepository repository = repository(occupiedSlots);
        UserApiKeyEntity account = account();
        BinanceClient client = binance(ApiResponse.<FuturesAssetBalanceClientResponse>builder()
                .statusCode(200)
                .data(FuturesAssetBalanceClientResponse.builder()
                        .asset("USDC")
                        .walletBalance("1000")
                        .availableBalance("500")
                        .marginAvailable(true)
                        .build())
                .build());
        MicroLiveCapacityProperties properties = properties();
        properties.setMaxConcurrentAllocations(0);

        MicroLiveCapacityDecision result = new MicroLiveCapacityGate(client, repository, properties)
                .evaluate(account, UUID.randomUUID(), "USDC");

        assertTrue(result.allowed());
        assertEquals(10, result.effectiveMaxSlots());
    }

    @Test
    void invalidCredentialsFailClosed() {
        AtomicLong occupied = new AtomicLong();
        UserCopyAllocationRepository repository = repository(occupied);
        BinanceClient client = binance(ApiResponse.<FuturesAssetBalanceClientResponse>builder()
                .statusCode(401).build());
        MicroLiveCapacityGate gate = new MicroLiveCapacityGate(client, repository, properties());
        UserApiKeyEntity account = account();
        MicroLiveCapacityDecision result = gate.evaluate(account, UUID.randomUUID(), "USDC");
        assertFalse(result.allowed());
        assertEquals("EXECUTION_ACCOUNT_CREDENTIALS_INVALID", result.reasonCode());
    }

    private static Fixture fixture(String walletBalance, String availableBalance, long occupied) {
        AtomicLong occupiedSlots = new AtomicLong(occupied);
        UserCopyAllocationRepository repository = repository(occupiedSlots);
        UserApiKeyEntity account = account();
        BinanceClient client = balanceClient(walletBalance, availableBalance);
        return new Fixture(new MicroLiveCapacityGate(client, repository, properties()), occupiedSlots, account);
    }

    private static MicroLiveCapacityGate gate(String walletBalance, String availableBalance, long occupied,
                                              MicroLiveCapacityProperties properties) {
        return new MicroLiveCapacityGate(balanceClient(walletBalance, availableBalance),
                repository(new AtomicLong(occupied)), properties);
    }

    private static BinanceClient balanceClient(String walletBalance, String availableBalance) {
        return binance(ApiResponse.<FuturesAssetBalanceClientResponse>builder()
                        .statusCode(200)
                        .data(FuturesAssetBalanceClientResponse.builder()
                                .asset("USDC")
                                .walletBalance(walletBalance)
                                .availableBalance(availableBalance)
                                .marginAvailable(true)
                        .build())
                        .build());
    }

    private static MicroLiveCapacityProperties properties() {
        MicroLiveCapacityProperties properties = new MicroLiveCapacityProperties();
        properties.setMaxConcurrentAllocations(5);
        properties.setBudgetPerAllocationUsdc(new java.math.BigDecimal("100"));
        return properties;
    }

    private static UserApiKeyEntity account() {
        UserApiKeyEntity account = new UserApiKeyEntity();
        account.setId(UUID.randomUUID());
        account.setApiKey("test-key");
        account.setApiSecret("test-secret");
        account.setExchange("BINANCE");
        account.setAccountPurpose(ExecutionAccountPurpose.MICRO_LIVE);
        account.setActive(true);
        return account;
    }

    private static UserCopyAllocationRepository repository(AtomicLong occupied) {
        return (UserCopyAllocationRepository) Proxy.newProxyInstance(
                UserCopyAllocationRepository.class.getClassLoader(),
                new Class<?>[]{UserCopyAllocationRepository.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("countOccupiedMicroLiveSlots")) return occupied.get();
                    if (method.getName().equals("toString")) return "capacity-repository-stub";
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static BinanceClient binance(ApiResponse<FuturesAssetBalanceClientResponse> response) {
        return (BinanceClient) Proxy.newProxyInstance(
                BinanceClient.class.getClassLoader(),
                new Class<?>[]{BinanceClient.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("assetBalance")) return response;
                    if (method.getName().equals("toString")) return "capacity-binance-stub";
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private record Fixture(MicroLiveCapacityGate gate,
                           AtomicLong occupied,
                           UserApiKeyEntity account) {
    }
}
