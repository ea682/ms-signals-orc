package com.apunto.engine.service.metric;

import java.time.Duration;
import java.util.Optional;

public interface MetricV2SnapshotPersistence {

    Optional<MetricV2SnapshotStore.Snapshot> load();

    void replace(MetricV2SnapshotStore.Snapshot snapshot, Duration maxStaleness);

    static MetricV2SnapshotPersistence noOp() {
        return new MetricV2SnapshotPersistence() {
            @Override
            public Optional<MetricV2SnapshotStore.Snapshot> load() {
                return Optional.empty();
            }

            @Override
            public void replace(MetricV2SnapshotStore.Snapshot snapshot, Duration maxStaleness) {
            }
        };
    }
}
