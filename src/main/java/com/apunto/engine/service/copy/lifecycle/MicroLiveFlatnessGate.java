package com.apunto.engine.service.copy.lifecycle;

@FunctionalInterface
public interface MicroLiveFlatnessGate {
    MicroLiveFlatness evaluate(Long allocationId);
}
