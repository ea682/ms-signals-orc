package com.apunto.engine.jobs;

import com.apunto.engine.service.ShadowPromotionService;
import com.apunto.engine.service.copy.promotion.ShadowPromotionResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShadowPromotionJobWorker {

    private final ShadowPromotionService shadowPromotionService;

    @Value("${copy.promotion.job.enabled:false}")
    private boolean enabled;

    @Scheduled(
            initialDelayString = "${copy.promotion.job.initial-delay-ms:30000}",
            fixedDelayString = "${copy.promotion.job.fixed-delay-ms:120000}"
    )
    public void promoteShadowToMicroLive() {
        if (!enabled) {
            return;
        }
        try {
            ShadowPromotionResult result = shadowPromotionService.promoteShadowToMicroLive();
            if (result.evaluated() > 0 || result.created() > 0 || result.rejected() > 0) {
                log.info(
                        "event=copy.promotion.shadow_to_micro.job_ok evaluated={} ready={} created={} rejected={} skipped={}",
                        result.evaluated(),
                        result.ready(),
                        result.created(),
                        result.rejected(),
                        result.skipped()
                );
            }
        } catch (DataAccessException | IllegalStateException | IllegalArgumentException e) {
            log.warn("event=copy.promotion.shadow_to_micro.failed errClass={} errMsg=\"{}\"",
                    e.getClass().getSimpleName(), safeLog(e.getMessage()));
        }
    }

    private static String safeLog(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }
}
