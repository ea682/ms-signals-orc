package com.apunto.engine.service;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;

public interface BinanceCopyExecutionService {

    void executeOpenForUser(OperacionEvent event, UserDetailDto userDetail);

    void executeCloseForUser(OperacionEvent event, UserDetailDto userDetail);
}