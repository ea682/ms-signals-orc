package com.apunto.copytarget;

/** Evidence authority for source margin used by Copy Policy V1. */
public enum MarginProvenance {
    EXPLICIT,
    DERIVED_CERTIFIED,
    UNAVAILABLE;

    public boolean usableForEntrySizing() {
        return this == EXPLICIT || this == DERIVED_CERTIFIED;
    }
}
