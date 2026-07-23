package com.apunto.engine.service.movement;

import java.time.OffsetDateTime;

/**
 * Total economic order for events that can share an exchange timestamp.
 */
public record MovementEconomicOrder(
        OffsetDateTime eventTime,
        Long sourceSequence,
        String deterministicId
) implements Comparable<MovementEconomicOrder> {
    private static final long MISSING_SEQUENCE = Long.MIN_VALUE;

    public MovementEconomicOrder {
        if (eventTime == null) {
            throw new IllegalArgumentException("eventTime is required");
        }
        sourceSequence = sourceSequence == null ? MISSING_SEQUENCE : sourceSequence;
        deterministicId = deterministicId == null ? "" : deterministicId;
    }

    @Override
    public int compareTo(MovementEconomicOrder other) {
        int time = eventTime.compareTo(other.eventTime);
        if (time != 0) {
            return time;
        }
        int sequence = Long.compare(sourceSequence, other.sourceSequence);
        if (sequence != 0) {
            return sequence;
        }
        return deterministicId.compareTo(other.deterministicId);
    }
}
