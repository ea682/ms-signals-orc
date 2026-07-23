package com.apunto.engine.jobs;

import com.apunto.engine.service.copy.certification.MicroLiveRecertificationProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MicroLiveRecertificationJobWorker {

    private final MicroLiveRecertificationProcessor processor;

    @Value("${copy.micro-live.recertification.enabled:false}")
    private boolean enabled;

    @Value("${copy.micro-live.recertification.batch-size:20}")
    private int batchSize;

    @Scheduled(initialDelayString = "${copy.micro-live.recertification.initial-delay-ms:30000}",
            fixedDelayString = "${copy.micro-live.recertification.fixed-delay-ms:15000}")
    @SchedulerLock(name = "copy-micro-live-recertification", lockAtMostFor = "PT5M", lockAtLeastFor = "PT1S")
    public void processPendingCapacity() {
        if (!enabled) return;
        int processed = 0;
        try {
            while (processed < Math.max(1, batchSize) && processor.processNext()) processed++;
            if (processed > 0) {
                log.info("event=copy.micro_live.recertification.batch_completed processed={} reasonCode=MICRO_LIVE_RECERTIFICATION_QUEUE_DRAINED",
                        processed);
            }
        } catch (RuntimeException error) {
            log.warn("event=copy.micro_live.recertification.batch_failed processed={} errorClass={}",
                    processed, error.getClass().getSimpleName());
        }
    }
}
