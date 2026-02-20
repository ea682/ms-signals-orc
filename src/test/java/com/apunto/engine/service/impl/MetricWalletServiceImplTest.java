package com.apunto.engine.service.impl;
import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.UserCopyAllocationService;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetricWalletServiceImplTest {

    private MetricWalletServiceImpl newService() {
        MetricWalletsInfoClient client = (limit, dayz) -> List.of();
        UserCopyAllocationService userCopyAllocationService = new UserCopyAllocationService() {
            @Override
            public void syncDistribution(int maxWallet, List<MetricaWalletDto> candidates) {
                // no-op for unit tests
            }

            @Override
            public List<UserCopyAllocationEntity> getActiveDistribution(int maxWallet) {
                return List.of();
            }
        };

        return new MetricWalletServiceImpl(
                client,
                userCopyAllocationService,
                30,
                1,
                1,
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                Duration.ofMillis(250)
        );
    }

    @Test
    void getMetricWallets_uses_selectCandidates_filters_and_allocates_capitalShare() {
        OffsetDateTime now = OffsetDateTime.now();

        MetricaWalletDto w1 = dto("w1", 10.0, 1, null, 90, null, null, 0.7);
        MetricaWalletDto w2 = dto("w2", 3.0, null, now, 80, null, null, 0.4);
        MetricaWalletDto w3 = dto("w3", 1.0, 1, null, 95, null, null, 0.9); // fuera por dayzLimit=3
        MetricaWalletDto w4 = dto("w4", 9.0, 1, null, 68, 85, null, 0.2);

        List<MetricaWalletDto> base = List.of(w1, w2, w3, w4);

        class Capture {
            int maxWallet;
            List<MetricaWalletDto> candidates;
            Integer dayzFromClient;
        }
        Capture cap = new Capture();

        MetricWalletsInfoClient client = (limit, dayz) -> {
            cap.dayzFromClient = dayz;
            return base;
        };
        UserCopyAllocationService userCopyAllocationService = new UserCopyAllocationService() {
            @Override
            public void syncDistribution(int maxWallet, List<MetricaWalletDto> candidates) {
                cap.maxWallet = maxWallet;
                cap.candidates = candidates;
            }

            @Override
            public List<UserCopyAllocationEntity> getActiveDistribution(int maxWallet) {
                return List.of();
            }
        };

        // dayzLimit=3 para forzar el filtro por historyDays
        MetricWalletServiceImpl svc = new MetricWalletServiceImpl(
                client,
                userCopyAllocationService,
                30,
                3,
                1,
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                Duration.ofMillis(250)
        );

        List<MetricaWalletDto> out = svc.getMetricWallets(3, 0.90, 0.50);

        assertEquals(3, out.size());
        assertEquals("w1", out.get(0).getWallet().getIdWallet());
        assertEquals("w2", out.get(1).getWallet().getIdWallet());
        assertEquals("w4", out.get(2).getWallet().getIdWallet());

        // El cliente se invoca con dayzLimit (config)
        assertEquals(3, cap.dayzFromClient);

        // Se intenta sincronizar la distribución (no debe explotar)
        assertEquals(3, cap.maxWallet);
        assertNotNull(cap.candidates);
        assertEquals(3, cap.candidates.size());

        // Allocation constraints
        double total = out.stream().mapToDouble(MetricaWalletDto::getCapitalShare).sum();
        assertTrue(total <= 0.90 + 1e-9);
        assertTrue(out.stream().allMatch(w -> w.getCapitalShare() <= 0.50 + 1e-9));
        assertTrue(out.stream().allMatch(w -> w.getCapitalShare() >= -1e-12));

        // Deben ser copias (no la misma instancia del input)
        assertNotSame(w1, out.get(0));
        assertNotSame(w2, out.get(1));
        assertNotSame(w4, out.get(2));
    }

    @SuppressWarnings("unchecked")
    private static List<MetricaWalletDto> invokeSelectCandidates(
            MetricWalletServiceImpl svc,
            List<MetricaWalletDto> base,
            int maxWallets,
            int dayzLimit
    ) {
        try {
            Method m = MetricWalletServiceImpl.class.getDeclaredMethod(
                    "selectCandidates",
                    List.class,
                    int.class,
                    int.class
            );
            m.setAccessible(true);
            return (List<MetricaWalletDto>) m.invoke(svc, base, maxWallets, dayzLimit);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static MetricaWalletDto dto(
            String walletId,
            Double historyDays,
            Integer countOperation,
            OffsetDateTime lastClosedAt,
            Integer conservative,
            Integer scalping,
            Integer aggressive,
            double capitalShare
    ) {
        return MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder()
                        .idWallet(walletId)
                        .historyDays(historyDays)
                        .countOperation(countOperation)
                        .build())
                .activity(lastClosedAt == null ? null : MetricaWalletDto.ActivityDto.builder()
                        .lastClosedAt(lastClosedAt)
                        .build())
                .scoring(MetricaWalletDto.ScoringDto.builder()
                        .decisionMetricConservative(conservative)
                        .decisionMetricScalping(scalping)
                        .decisionMetricAggressive(aggressive)
                        .build())
                .capitalShare(capitalShare)
                .build();
    }

    @Test
    void selectCandidates_filters_sorts_limits_and_resetsCapitalShare() {
        MetricWalletServiceImpl svc = newService();

        OffsetDateTime now = OffsetDateTime.now();

        // ✅ Pasa (cons >= 69, historyDays >= 3, closed by countOperation)
        MetricaWalletDto w1 = dto("w1", 10.0, 1, null, 90, null, null, 0.7);

        // ✅ Pasa (cons >= 69, historyDays >= 3, closed by lastClosedAt)
        MetricaWalletDto w2 = dto("w2", 3.0, null, now, 80, null, null, 0.4);

        // ❌ Falla por dayzLimit (historyDays < 3)
        MetricaWalletDto w3 = dto("w3", 1.0, 1, null, 95, null, null, 0.9);

        // ✅ Pasa por scalping (>= 80) aunque cons < 69 (se ordena por cons)
        MetricaWalletDto w4 = dto("w4", 9.0, 1, null, 68, 85, null, 0.2);

        // ❌ scoring null
        MetricaWalletDto w5 = MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder().idWallet("w5").historyDays(10.0).countOperation(1).build())
                .scoring(null)
                .capitalShare(0.8)
                .build();

        // ❌ historyDays null
        MetricaWalletDto w6 = dto("w6", null, 1, null, 90, null, null, 0.8);

        List<MetricaWalletDto> base = new ArrayList<>();
        base.add(null);
        base.add(w6);
        base.add(w5);
        base.add(w4);
        base.add(w3);
        base.add(w2);
        base.add(w1);

        List<MetricaWalletDto> out = invokeSelectCandidates(svc, base, 3, 3);

        assertEquals(3, out.size());
        assertEquals("w1", out.get(0).getWallet().getIdWallet());
        assertEquals("w2", out.get(1).getWallet().getIdWallet());
        assertEquals("w4", out.get(2).getWallet().getIdWallet());

        // BeanUtils.copyProperties => instancia nueva (copia superficial)
        assertNotSame(w1, out.get(0));
        assertNotSame(w2, out.get(1));
        assertNotSame(w4, out.get(2));

        // copyForAllocation resetea capitalShare
        assertEquals(0.0, out.get(0).getCapitalShare(), 1e-12);
        assertEquals(0.0, out.get(1).getCapitalShare(), 1e-12);
        assertEquals(0.0, out.get(2).getCapitalShare(), 1e-12);
    }

    @Test
    void selectCandidates_includes_wallet_passing_aggressive_threshold_even_if_conservative_is_null() {
        MetricWalletServiceImpl svc = newService();

        // Pasa por aggressive >= 80; decisionScore() será 0 (cons null), pero debe incluirse si no hay limit que lo saque.
        MetricaWalletDto wAgg = dto("agg", 10.0, 1, null, null, null, 90, 0.3);
        MetricaWalletDto wCons = dto("cons", 10.0, 1, null, 70, null, null, 0.3);

        List<MetricaWalletDto> out = invokeSelectCandidates(svc, List.of(wAgg, wCons), 10, 1);

        assertEquals(2, out.size());
        assertEquals("cons", out.get(0).getWallet().getIdWallet()); // score 70
        assertEquals("agg", out.get(1).getWallet().getIdWallet());  // score 0
    }

    @Test
    void selectCandidates_excludes_wallets_without_closed_history() {
        MetricWalletServiceImpl svc = newService();

        // No lastClosedAt y countOperation=0 => hasClosedHistory=false
        MetricaWalletDto openOnly = dto("open", 10.0, 0, null, 90, null, null, 0.5);

        // Con lastClosedAt => hasClosedHistory=true
        MetricaWalletDto closed = dto("closed", 10.0, 0, OffsetDateTime.now(), 90, null, null, 0.5);

        List<MetricaWalletDto> out = invokeSelectCandidates(svc, List.of(openOnly, closed), 10, 1);

        assertEquals(1, out.size());
        assertEquals("closed", out.get(0).getWallet().getIdWallet());
    }
}
