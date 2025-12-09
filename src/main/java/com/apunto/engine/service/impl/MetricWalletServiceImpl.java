package com.apunto.engine.service.impl;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.service.MetricWalletService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@AllArgsConstructor
public class MetricWalletServiceImpl implements MetricWalletService {

    private final MetricWalletsInfoClient metricWalletsInfoClient;


    @Override
    public List<MetricaWalletDto> getMetricWallets(int maxWallets) {
        double maxCapitalToUse = 0.90;
        double maxPerWallet    = 0.50;
        return getMetricWallets(maxWallets, maxCapitalToUse, maxPerWallet);
    }

    public List<MetricaWalletDto> getMetricWallets(int maxWallets,
                                                   double maxCapitalToUse,
                                                   double maxPerWallet) {

        if (maxCapitalToUse <= 0.0 || maxCapitalToUse > 1.0) {
            throw new IllegalArgumentException("maxCapitalToUse debe estar entre 0 y 1 (ej: 0.9 = 90%).");
        }
        if (maxPerWallet <= 0.0 || maxPerWallet > maxCapitalToUse) {
            throw new IllegalArgumentException("maxPerWallet debe estar entre 0 y maxCapitalToUse.");
        }

        List<MetricaWalletDto> metricWalletDtos = metricWalletsInfoClient.allPositionHistory(60);

        List<MetricaWalletDto> candidates = metricWalletDtos.stream()
                .filter(MetricaWalletDto::getPassesFilter)
                .sorted(Comparator.comparingDouble(MetricaWalletDto::getDecisionMetric).reversed())
                .limit(maxWallets)
                .collect(Collectors.toList());

        applyCapitalWeights(candidates, maxPerWallet, maxCapitalToUse);

        validateLimits(candidates, maxPerWallet, maxCapitalToUse);

        return candidates;
    }

    private void applyCapitalWeights(List<MetricaWalletDto> wallets,
                                     double maxPerWallet,
                                     double maxCapitalToUse) {
        if (wallets == null || wallets.isEmpty()) {
            return;
        }

        wallets.forEach(w -> w.setCapitalShare(0.0));

        double remainingWeight = maxCapitalToUse;
        List<MetricaWalletDto> remaining = new ArrayList<>(wallets);

        while (!remaining.isEmpty() && remainingWeight > 0.0) {
            double sumScore = remaining.stream()
                    .mapToDouble(MetricaWalletDto::getDecisionMetric)
                    .sum();

            if (sumScore <= 0.0) {
                double equalShare = remainingWeight / remaining.size();
                Iterator<MetricaWalletDto> itEqual = remaining.iterator();
                while (itEqual.hasNext() && remainingWeight > 0.0) {
                    MetricaWalletDto w = itEqual.next();
                    double current = w.getCapitalShare();
                    double room = maxPerWallet - current;
                    if (room <= 0.0) {
                        itEqual.remove();
                        continue;
                    }
                    double increment = Math.min(equalShare, room);
                    w.setCapitalShare(current + increment);
                    remainingWeight -= increment;

                    if (room <= increment) {
                        itEqual.remove();
                    }
                }
                break;
            }

            boolean anyCapped = false;

            Iterator<MetricaWalletDto> it = remaining.iterator();
            while (it.hasNext() && remainingWeight > 0.0) {
                MetricaWalletDto w = it.next();
                double score = w.getDecisionMetric();

                double proposed = remainingWeight * (score / sumScore);
                double current  = w.getCapitalShare();
                double newShare = current + proposed;

                if (newShare > maxPerWallet) {
                    double cappedIncrement = maxPerWallet - current;
                    if (cappedIncrement < 0.0) {
                        cappedIncrement = 0.0;
                    }

                    w.setCapitalShare(current + cappedIncrement);
                    remainingWeight -= cappedIncrement;
                    it.remove();
                    anyCapped = true;
                }
            }

            if (!anyCapped && remainingWeight > 0.0) {
                for (MetricaWalletDto w : remaining) {
                    double score = w.getDecisionMetric();
                    double shareIncrement = remainingWeight * (score / sumScore);
                    double current = w.getCapitalShare();
                    double newShare = current + shareIncrement;

                    if (newShare > maxPerWallet) {
                        shareIncrement = maxPerWallet - current;
                        newShare = maxPerWallet;
                    }
                    w.setCapitalShare(newShare);
                }
                remainingWeight = 0.0;
                break;
            }
        }

        wallets.forEach(w -> {
            if (w.getCapitalShare() < 0.0) {
                w.setCapitalShare(0.0);
            }
        });

        double total = wallets.stream()
                .mapToDouble(MetricaWalletDto::getCapitalShare)
                .sum();

        log.debug("Total capitalShare asignado = {}, maxCapitalToUse = {}", total, maxCapitalToUse);
    }

    private void validateLimits(List<MetricaWalletDto> wallets,
                                double maxPerWallet,
                                double maxCapitalToUse) {

        double total = wallets.stream()
                .mapToDouble(MetricaWalletDto::getCapitalShare)
                .sum();

        double max = wallets.stream()
                .mapToDouble(MetricaWalletDto::getCapitalShare)
                .max()
                .orElse(0.0);

        if (total - maxCapitalToUse > 1e-9) {
            throw new IllegalStateException(
                    String.format("Total capitalShare (%.6f) excede maxCapitalToUse (%.6f)", total, maxCapitalToUse)
            );
        }

        if (max - maxPerWallet > 1e-9) {
            throw new IllegalStateException(
                    String.format("Una wallet tiene capitalShare (%.6f) mayor a maxPerWallet (%.6f)", max, maxPerWallet)
            );
        }
    }
}
