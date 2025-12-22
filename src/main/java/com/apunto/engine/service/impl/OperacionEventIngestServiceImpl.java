package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.service.CopyExecutionJobService;
import com.apunto.engine.service.OperacionEventIngestService;
import com.apunto.engine.service.UserDetailCachedService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class OperacionEventIngestServiceImpl implements OperacionEventIngestService {

    private static final String ERR_EVENT_NULL = "El evento no puede ser null";
    private static final String ERR_TIPO_NULL = "El tipo del evento no puede ser null";
    private static final String ERR_OPERACION_NULL = "La operacion no puede ser null";
    private static final String ERR_ID_NULL = "El idOperacion no puede ser null";

    private static final String LOG_ENQUEUED =
            "event=copy.execution.enqueued originId={} tipo={} usersCached={} enqueued={}";

    private final UserDetailCachedService userDetailCachedService;
    private final CopyExecutionJobService copyExecutionJobService;

    @Override
    public int ingest(OperacionEvent event) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(event.getTipo(), ERR_TIPO_NULL);
        Objects.requireNonNull(event.getOperacion(), ERR_OPERACION_NULL);
        Objects.requireNonNull(event.getOperacion().getIdOperacion(), ERR_ID_NULL);

        final String originId = event.getOperacion().getIdOperacion().toString();
        final CopyJobAction action = mapAction(event.getTipo());

        final List<UserDetailDto> usersCached = safeUsers(userDetailCachedService.getUsers());
        final int enqueued = copyExecutionJobService.enqueueForUsers(event, usersCached, action);

        log.info(LOG_ENQUEUED, originId, event.getTipo(), usersCached.size(), enqueued);
        return enqueued;
    }

    private CopyJobAction mapAction(OperacionEvent.Tipo tipo) {
        return switch (tipo) {
            case ABIERTA -> CopyJobAction.OPEN;
            case CERRADA -> CopyJobAction.CLOSE;
        };
    }

    private List<UserDetailDto> safeUsers(List<UserDetailDto> users) {
        return users == null ? Collections.emptyList() : users;
    }
}
