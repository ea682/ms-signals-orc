package com.apunto.engine.service.copy.account;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class BinanceClientWiringContractTest {

    @Test
    void productionAccountServicesSelectTheInformationClientWhenBothClientsExist() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean("binanceInfoClient", BinanceClient.class, () -> proxy(BinanceClient.class));
            context.registerBean("binanceCloseClient", BinanceClient.class, () -> proxy(BinanceClient.class));
            context.registerBean(UserCopyAllocationRepository.class,
                    () -> proxy(UserCopyAllocationRepository.class));
            context.registerBean(UserApiKeyRepository.class, () -> proxy(UserApiKeyRepository.class));
            context.registerBean(MicroLiveCapacityProperties.class, MicroLiveCapacityProperties::new);
            context.registerBean(MicroLiveCapacitySnapshotStore.class,
                    () -> proxy(MicroLiveCapacitySnapshotStore.class));
            context.registerBean(MicroLiveCapacityGate.class);
            context.registerBean(ExecutionAccountIsolationVerifier.class);

            context.refresh();

            assertNotNull(context.getBean(MicroLiveCapacityGate.class));
            assertNotNull(context.getBean(ExecutionAccountIsolationVerifier.class));
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (instance, method, args) -> {
                    if (method.getReturnType() == boolean.class) return false;
                    if (method.getReturnType() == int.class) return 0;
                    if (method.getReturnType() == long.class) return 0L;
                    return null;
                });
    }
}
