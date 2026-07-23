package com.apunto.engine.jobs;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.copy.certification.AutomaticLiveAdoptionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(prefix = "copy.live-adoption.reconciliation", name = "enabled",
        havingValue = "true", matchIfMissing = false)
public class LiveAdoptionReconciliationWorker {

    private final UserCopyAllocationRepository allocationRepository;
    private final AutomaticLiveAdoptionService adoptionService;

    @Scheduled(initialDelayString = "${copy.live-adoption.reconciliation.initial-delay-ms:15000}",
            fixedDelayString = "${copy.live-adoption.reconciliation.fixed-delay-ms:60000}")
    @SchedulerLock(name = "copy-live-adoption-reconciliation", lockAtMostFor = "PT5M", lockAtLeastFor = "PT1S")
    public void reconcile() {
        for (UserCopyAllocationEntity allocation : allocationRepository
                .findPendingLiveAdoptionReconciliation(50)) {
            try {
                adoptionService.reconcile(allocation);
            } catch (RuntimeException ex) {
                log.error("event=copy.live_adoption.reconciliation_failed allocationId={} userId={} errClass={} reasonCode=LIVE_ADOPTION_RECONCILIATION_FAILED",
                        allocation.getId(), allocation.getIdUser(), ex.getClass().getSimpleName());
            }
        }
    }
}
