package com.apunto.copytarget;

import java.util.Objects;

/**
 * Defines when absence from a source portfolio can safely imply a target exit.
 */
public final class SourcePortfolioAuthorityPolicy {

    private SourcePortfolioAuthorityPolicy() {
    }

    public static boolean canInferAbsenceClose(
            boolean complete,
            Long sourceSnapshotVersion,
            Long portfolioSnapshotVersion,
            int advertisedPositions,
            int resolvedPositions
    ) {
        return complete
                && sourceSnapshotVersion != null
                && Objects.equals(sourceSnapshotVersion, portfolioSnapshotVersion)
                && advertisedPositions >= 0
                && resolvedPositions >= 0
                && advertisedPositions == resolvedPositions;
    }
}
