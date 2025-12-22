package com.apunto.engine.service;

import com.apunto.engine.events.OperacionEvent;

public interface OperacionEventIngestService {
    int ingest(OperacionEvent event);
}
