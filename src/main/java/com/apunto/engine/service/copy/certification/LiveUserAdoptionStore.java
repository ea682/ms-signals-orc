package com.apunto.engine.service.copy.certification;

public interface LiveUserAdoptionStore {
    void upsert(UserAdoptionValidationRequest request, UserAdoptionValidationDecision decision);
}
