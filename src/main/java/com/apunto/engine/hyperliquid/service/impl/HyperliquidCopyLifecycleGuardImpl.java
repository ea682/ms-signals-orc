package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.model.HyperliquidCopyLifecycleDecision;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.hyperliquid.service.HyperliquidCopyLifecycleGuard;
import com.apunto.engine.jobs.model.CopyJobAction;
import org.springframework.stereotype.Service;

@Service
public class HyperliquidCopyLifecycleGuardImpl implements HyperliquidCopyLifecycleGuard {

    @Override
    public HyperliquidCopyLifecycleDecision decide(CopyJobAction action, HyperliquidDeltaType deltaType, boolean activeCopy) {
        if (action == null) {
            return HyperliquidCopyLifecycleDecision.skip("copy_action_missing", activeCopy);
        }
        HyperliquidDeltaType effectiveDelta = deltaType == null ? HyperliquidDeltaType.UNKNOWN : deltaType;

        if (action == CopyJobAction.CLOSE) {
            return activeCopy
                    ? HyperliquidCopyLifecycleDecision.allow(true)
                    : HyperliquidCopyLifecycleDecision.skip("close_without_open_copy", false);
        }

        if (action != CopyJobAction.OPEN) {
            return HyperliquidCopyLifecycleDecision.skip("copy_action_unsupported", activeCopy);
        }

        if (effectiveDelta.isOpen()) {
            return activeCopy
                    ? HyperliquidCopyLifecycleDecision.skip("open_copy_already_active", true)
                    : HyperliquidCopyLifecycleDecision.allow(false);
        }

        if (effectiveDelta.canAdjustExistingCopy()) {
            return activeCopy
                    ? HyperliquidCopyLifecycleDecision.allow(true)
                    : HyperliquidCopyLifecycleDecision.skip(adjustmentReason(effectiveDelta), false);
        }

        return HyperliquidCopyLifecycleDecision.skip("delta_not_copyable", activeCopy);
    }

    private String adjustmentReason(HyperliquidDeltaType deltaType) {
        return switch (deltaType) {
            case FLIP -> "flip_without_open_copy";
            case UPDATE -> "update_without_open_copy";
            case RESIZE -> "resize_without_open_copy";
            default -> "adjustment_without_open_copy";
        };
    }
}
