package com.apunto.engine.service.copy.lifecycle;

import com.apunto.engine.repository.CopyDispatchIntentRepository;
import com.apunto.engine.repository.CopyOperationRepository;
import org.springframework.stereotype.Service;

@Service
public class PostgresMicroLiveFlatnessGate implements MicroLiveFlatnessGate {

    private final CopyOperationRepository operationRepository;
    private final CopyDispatchIntentRepository dispatchRepository;

    public PostgresMicroLiveFlatnessGate(CopyOperationRepository operationRepository,
                                         CopyDispatchIntentRepository dispatchRepository) {
        this.operationRepository = operationRepository;
        this.dispatchRepository = dispatchRepository;
    }

    @Override
    public MicroLiveFlatness evaluate(Long allocationId) {
        if (allocationId == null) return new MicroLiveFlatness(false, 0, 0);
        long positions = operationRepository.countByUserCopyAllocationIdAndActiveTrue(allocationId);
        long dispatches = dispatchRepository.countNonTerminalByAllocationId(allocationId);
        return new MicroLiveFlatness(positions == 0 && dispatches == 0, positions, dispatches);
    }
}
