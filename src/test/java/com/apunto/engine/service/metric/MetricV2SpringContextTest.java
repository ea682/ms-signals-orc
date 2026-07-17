package com.apunto.engine.service.metric;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.service.copy.decision.MetricCopyDecisionGateway;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricV2SpringContextTest {

    @Test
    void canonicalV2BeansAndSafetyPropertiesWireInSpringContext() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getBeanFactory().setConversionService(ApplicationConversionService.getSharedInstance());
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource(
                    "metric-v2-test",
                    Map.of(
                            "metric-wallet.read-mode", "V2",
                            "metric-wallet.v2.fail-open-new-exposure", "false",
                            "metric-wallet.v2.allow-close-on-failure", "true",
                            "metric-wallet.v2.shadow-auto-enroll-enabled", "false"
                    )
            ));
            context.registerBean(MetricWalletsInfoClient.class, MetricV2SpringContextTest::noCallClient);
            context.registerBean(MeterRegistry.class, SimpleMeterRegistry::new);
            context.registerBean(MetricV2SnapshotPersistence.class, MetricV2SnapshotPersistence::noOp);
            context.registerBean(MetricStrategyShadowProjectionMapper.class);
            context.registerBean(MetricWalletReadModeResolver.class);
            context.registerBean(MetricV2SnapshotStore.class);
            context.registerBean(MetricCopyDecisionGateway.class);

            context.refresh();

            assertEquals(MetricWalletReadMode.V2, context.getBean(MetricWalletReadModeResolver.class).effectiveMode());
            assertTrue(context.getBean(MetricV2SnapshotStore.class).allowCloseOnFailure());
            assertNotNull(context.getBean(MetricCopyDecisionGateway.class));
        }
    }

    private static MetricWalletsInfoClient noCallClient() {
        return (MetricWalletsInfoClient) Proxy.newProxyInstance(
                MetricWalletsInfoClient.class.getClassLoader(),
                new Class<?>[]{MetricWalletsInfoClient.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "NoCallMetricWalletsInfoClient";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> null;
                        };
                    }
                    if (List.class.isAssignableFrom(method.getReturnType())) return List.of();
                    throw new AssertionError("Metric HTTP call reached during context wiring: " + method.getName());
                }
        );
    }
}
