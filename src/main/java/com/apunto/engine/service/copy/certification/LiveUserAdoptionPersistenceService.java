package com.apunto.engine.service.copy.certification;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class LiveUserAdoptionPersistenceService {

    private final LiveUserAdoptionValidator validator;
    private final LiveUserAdoptionStore store;

    @Transactional
    public UserAdoptionValidationDecision validateAndPersist(UserAdoptionValidationRequest request) {
        UserAdoptionValidationDecision decision = validator.validate(request);
        store.upsert(request, decision);
        return decision;
    }
}
