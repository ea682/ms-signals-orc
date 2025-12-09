package com.apunto.engine.service;

import com.apunto.engine.events.OperacionEvent;


public interface OperationCoreService {
    void procesarEventoOperacion(OperacionEvent event);
}
