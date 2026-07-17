package com.apunto.engine.service.copy.certification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class ManualLiveCertificationCatalogService {

    private static final Set<LiveCertificationStatus> INITIAL_STATES = EnumSet.of(
            LiveCertificationStatus.SOURCE_SHADOW_VALIDATING,
            LiveCertificationStatus.EXECUTABLE_SHADOW_VALIDATING,
            LiveCertificationStatus.MICRO_LIVE_VALIDATING);

    private final LiveCertificationCatalogStore catalogStore;
    private final LiveCertificationTransitionStore transitionStore;
    private final Clock clock;

    @Autowired
    public ManualLiveCertificationCatalogService(LiveCertificationCatalogStore catalogStore,
                                                 LiveCertificationTransitionStore transitionStore) {
        this(catalogStore, transitionStore, Clock.systemUTC());
    }

    ManualLiveCertificationCatalogService(LiveCertificationCatalogStore catalogStore,
                                           LiveCertificationTransitionStore transitionStore,
                                           Clock clock) {
        this.catalogStore = catalogStore;
        this.transitionStore = transitionStore;
        this.clock = clock;
    }

    @Transactional
    public LiveCertificationCreateResult create(LiveCertificationCreateCommand command) {
        String validation = validate(command);
        if (validation != null) return LiveCertificationCreateResult.blocked(validation);

        LiveCertificationCatalogRecord byKey = catalogStore
                .findByCreationKey(command.creationKey().trim()).orElse(null);
        if (byKey != null) {
            return sameCreation(byKey, command)
                    ? LiveCertificationCreateResult.created(byKey.snapshot(), true)
                    : LiveCertificationCreateResult.blocked("LIVE_CERTIFICATION_CREATION_KEY_CONFLICT");
        }
        if (catalogStore.findByIdentity(command.identity()).isPresent()) {
            return LiveCertificationCreateResult.blocked("LIVE_CERTIFICATION_IDENTITY_ALREADY_EXISTS");
        }

        LiveCertificationCatalogRecord record = new LiveCertificationCatalogRecord(
                UUID.randomUUID(), command.identity(), command.evidenceLevel(),
                command.initialStatus(), 0L);
        boolean inserted = catalogStore.insert(record, command.creationKey().trim(),
                Map.copyOf(command.evidenceSnapshot()), command.actor().trim(), command.reason().trim());
        if (!inserted) {
            LiveCertificationCatalogRecord raced = catalogStore
                    .findByCreationKey(command.creationKey().trim()).orElse(null);
            return raced != null && sameCreation(raced, command)
                    ? LiveCertificationCreateResult.created(raced.snapshot(), true)
                    : LiveCertificationCreateResult.blocked("LIVE_CERTIFICATION_CREATE_CONFLICT");
        }
        transitionStore.appendAudit(new LiveCertificationAuditFact(
                record.id(), "CREATE:" + command.creationKey().trim(), record.status(), record.status(),
                0L, 0L, command.actor().trim(), command.reason().trim(),
                Map.copyOf(command.evidenceSnapshot()), OffsetDateTime.now(clock)));
        return LiveCertificationCreateResult.created(record.snapshot(), false);
    }

    private static String validate(LiveCertificationCreateCommand command) {
        if (command == null || command.identity() == null || command.evidenceLevel() == null
                || command.initialStatus() == null || command.actor() == null || command.actor().isBlank()
                || command.reason() == null || command.reason().isBlank()
                || command.creationKey() == null || command.creationKey().isBlank()
                || command.evidenceSnapshot() == null || command.evidenceSnapshot().isEmpty()) {
            return "LIVE_CERTIFICATION_CREATE_CONTRACT_INVALID";
        }
        if (!INITIAL_STATES.contains(command.initialStatus())) {
            return "LIVE_CERTIFICATION_INITIAL_STATUS_INVALID";
        }
        return null;
    }

    private static boolean sameCreation(LiveCertificationCatalogRecord record,
                                        LiveCertificationCreateCommand command) {
        return record.identity().equals(command.identity())
                && record.evidenceLevel() == command.evidenceLevel()
                && record.status() == command.initialStatus();
    }
}
