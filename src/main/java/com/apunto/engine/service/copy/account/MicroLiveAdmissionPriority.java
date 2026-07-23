package com.apunto.engine.service.copy.account;

public enum MicroLiveAdmissionPriority {
    SHADOW_PROMOTION,
    RECERTIFICATION;

    public boolean recertification() {
        return this == RECERTIFICATION;
    }
}
