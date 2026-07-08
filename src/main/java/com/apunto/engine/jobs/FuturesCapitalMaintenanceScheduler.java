package com.apunto.engine.jobs;

import com.apunto.engine.service.futures.FuturesCapitalMaintenanceService;
import com.apunto.engine.shared.exception.EngineException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FuturesCapitalMaintenanceScheduler {

    private final FuturesCapitalMaintenanceService maintenanceService;

    @Value("${futures.capital-maintenance.enabled:true}")
    private boolean enabled;

    @Scheduled(
            initialDelayString = "${futures.capital-maintenance.initial-delay-ms:30000}",
            fixedDelayString = "${futures.capital-maintenance.fixed-delay-ms:6000}"
    )
    public void run() {
        if (!enabled) {
            log.debug("event=futures.capital_maintenance.scheduler.skip reasonCode=disabled friendlyStep=el_scheduler_esta_apagado_por_configuracion");
            return;
        }
        try {
            maintenanceService.maintainAllActiveUsersCapital();
        } catch (EngineException | DataAccessException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
            log.warn("event=futures.capital_maintenance.scheduler.fail errClass={} errMsg=\"{}\" friendlyStep=fallo_el_ciclo_completo_y_se_reintentara_en_el_proximo_turno",
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
        }
    }

    private String safeLog(String value) {
        if (value == null) {
            return "";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }
}
