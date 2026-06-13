package com.apunto.engine.service;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.events.OperacionEvent;

public interface BinanceCopyExecutionService {

    void executeOpenForUser(OperacionEvent event, UserDetailDto userDetail);

    default void executeOpenForUser(OperacionEvent event, UserDetailDto userDetail, UserCopyAllocationEntity allocation) {
        executeOpenForUser(event, userDetail);
    }

    void executeCloseForUser(OperacionEvent event, UserDetailDto userDetail);

    default void executeCloseForUser(OperacionEvent event, UserDetailDto userDetail, UserCopyAllocationEntity allocation, String strategyCode) {
        executeCloseForUser(event, userDetail);
    }
}
