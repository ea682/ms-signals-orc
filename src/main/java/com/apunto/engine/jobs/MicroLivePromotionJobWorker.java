package com.apunto.engine.jobs;

import com.apunto.engine.service.MicroLivePromotionService;
import com.apunto.engine.service.copy.promotion.LivePromotionResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MicroLivePromotionJobWorker {

    private final MicroLivePromotionService microLivePromotionService;

    @Value("${copy.live-promotion.job.enabled:false}")
    private boolean enabled;

    @Scheduled(
            initialDelayString = "${copy.live-promotion.job.initial-delay-ms:45000}",
            fixedDelayString = "${copy.live-promotion.job.fixed-delay-ms:300000}"
    )
    public void promoteMicroLiveToLive() {
        if (!enabled) {
            return;
        }
        try {
            LivePromotionResult result = microLivePromotionService.promoteMicroLiveToLive();
            if (result.evaluated() > 0 || result.promoted() > 0 || result.rejected() > 0) {
                log.info(
                        "event=copy.promotion.micro_to_live.job_ok evaluated={} ready={} promoted={} rejected={} skipped={}",
                        result.evaluated(),
                        result.ready(),
                        result.promoted(),
                        result.rejected(),
                        result.skipped()
                );
            }
        } catch (DataAccessException | IllegalStateException | IllegalArgumentException e) {
            log.warn("event=copy.promotion.micro_to_live.job_failed errClass={} errMsg=\"{}\"",
                    e.getClass().getSimpleName(), safeLog(e.getMessage()));
        }
    }

    private static String safeLog(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }
}
