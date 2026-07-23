package com.apunto.engine.service.copy.account;

@FunctionalInterface
public interface MicroLiveCapacitySnapshotStore {

    void save(MicroLiveCapacitySnapshot snapshot);
}
