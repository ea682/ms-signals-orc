package com.apunto.engine.controller;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.service.BinanceEngineService;
import com.apunto.engine.service.UserDetailCachedService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/operaciones")
public class OperacionEventController {

    private final UserDetailCachedService userDetailCachedService;
    private final BinanceEngineService binanceEngineService;

    public OperacionEventController(
            UserDetailCachedService userDetailCachedService,
            BinanceEngineService binanceEngineService
    ) {
        this.userDetailCachedService = userDetailCachedService;
        this.binanceEngineService = binanceEngineService;
    }

    @PostMapping("/proce-operation")
    public ResponseEntity<Void> procesarEventoOperacion(@RequestBody OperacionEvent event) {
        List<UserDetailDto> usersDetail = userDetailCachedService.getUsers();

        if ("ABIERTA".equals(event.getTipo().name())) {
            binanceEngineService.openOperation(event, usersDetail);
        } else {
            binanceEngineService.closeOperation(event, usersDetail);
        }

        return ResponseEntity.accepted().build();
    }
}
