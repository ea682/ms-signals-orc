package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.service.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
@Slf4j
@AllArgsConstructor
public class OperationCoreServiceImpl implements OperationCoreService {

    private final UserDetailCachedService userDetailCachedService;

    private final BinanceEngineService binanceEngineService;

    @Override
    public void procesarEventoOperacion(OperacionEvent event) {

        List<UserDetailDto> usersDetail = userDetailCachedService.getUsers();


        if("ABIERTA".equals(event.getTipo().name())){
            binanceEngineService.openOperation(event, usersDetail);
        }else{
            binanceEngineService.closeOperation(event, usersDetail);
        }

    }
}
