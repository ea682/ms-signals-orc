package com.apunto.engine.service;


import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;

import java.util.List;

public interface BinanceEngineService {

    void openOperation(OperacionEvent event, List<UserDetailDto> usersDetail);
    void closeOperation(OperacionEvent event, List<UserDetailDto> usersDetail);
}
