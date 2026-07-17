package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.TargetPortfolioRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
@RequiredArgsConstructor
public class CopySimulationSubmissionService {

    private final CopySimulationJobStore store;

    public CopySimulationSubmissionResult submit(CopySimulationContext context, TargetPortfolioRequest request) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(request, "request");
        if (!context.isMicroLive()) {
            return CopySimulationSubmissionResult.NOT_APPLICABLE;
        }
        boolean inserted = store.enqueue(context, CopySimulationInputSnapshot.from(request));
        return inserted
                ? CopySimulationSubmissionResult.ENQUEUED
                : CopySimulationSubmissionResult.ALREADY_ENQUEUED;
    }
}
