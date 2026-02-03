package com.apunto.engine.client;

import com.apunto.engine.dto.client.MetricaWalletDto;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;

import java.util.List;

@HttpExchange(contentType = "application/json")
public interface MetricWalletsInfoClient {

    @GetExchange("/operaciones/metrica")
    List<MetricaWalletDto> allPositionHistory(@RequestParam("limit") int limit, @RequestParam("dayz") int dayz);
}
