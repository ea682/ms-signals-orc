package com.apunto.engine.client;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.dto.client.CopyGuardWindowSnapshotDto;
import com.apunto.engine.dto.client.CopyDecisionDto;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;

import java.util.List;

@HttpExchange(contentType = "application/json")
public interface MetricWalletsInfoClient {

    @GetExchange("/operaciones/metrica")
    List<MetricaWalletDto> allPositionHistory(@RequestParam("limit") int limit, @RequestParam("dayz") int dayz);

    @GetExchange("/operaciones/metrica/joyas")
    List<MetricaWalletDto> joyas(
            @RequestParam("limit") int limit,
            @RequestParam("dayz") int dayz,
            @RequestParam("simulation") String simulation
    );

    @GetExchange("/operaciones/metrica/copy-guard/windows")
    List<CopyGuardWindowSnapshotDto> copyGuardWindows(
            @RequestParam("limit") int limit,
            @RequestParam("dayz") int dayz,
            @RequestParam("mode") String mode,
            @RequestParam("windows") String windows
    );

    @GetExchange("/operaciones/metrica/copy-decision")
    CopyDecisionDto copyDecision(
            @RequestParam("walletId") String walletId,
            @RequestParam("strategyCode") String strategyCode,
            @RequestParam("scopeType") String scopeType,
            @RequestParam("scopeValue") String scopeValue,
            @RequestParam("mode") String mode,
            @RequestParam("simulation") String simulation,
            @RequestParam("minHistoryDays") int minHistoryDays,
            @RequestParam("simulationLookbackDays") int simulationLookbackDays,
            @RequestParam("maxFactsPerUnit") int maxFactsPerUnit,
            @RequestParam("timeoutMs") int timeoutMs,
            @RequestParam("debug") boolean debug
    );
}
