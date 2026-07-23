package com.apunto.engine.service.impl;

import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserRepository;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UserDetailServiceImplTest {

    @Test
    void inactiveAccountIsNeverReturnedEvenWhenDetailAndBinanceKeyAreActive() {
        UUID userId = UUID.randomUUID();
        UserEntity user = new UserEntity();
        user.setId(userId);
        user.setActivo(false);

        DetailUserEntity detail = new DetailUserEntity();
        detail.setUser(user);
        detail.setUserActive(true);
        detail.setApiKeyBinar(true);

        UserApiKeyEntity key = new UserApiKeyEntity();
        key.setExchange("BINANCE");
        key.setApiKey("key");
        key.setApiSecret("secret");

        AtomicInteger detailReads = new AtomicInteger();
        AtomicInteger keyReads = new AtomicInteger();
        UserRepository users = proxy(UserRepository.class, (proxy, method, args) ->
                "findAll".equals(method.getName()) ? List.of(user) : defaultValue(method.getReturnType()));
        DetailUserRepository details = proxy(DetailUserRepository.class, (proxy, method, args) -> {
            if ("findByUser_Id_AndUserActive".equals(method.getName())) {
                detailReads.incrementAndGet();
                return detail;
            }
            return defaultValue(method.getReturnType());
        });
        UserApiKeyRepository keys = proxy(UserApiKeyRepository.class, (proxy, method, args) -> {
            if ("findByUser_Id".equals(method.getName())) {
                keyReads.incrementAndGet();
                return key;
            }
            return defaultValue(method.getReturnType());
        });

        List<?> result = new UserDetailServiceImpl(users, details, keys).findAllActive();

        assertEquals(List.of(), result);
        assertEquals(0, detailReads.get(), "inactive users must be rejected before dependent reads");
        assertEquals(0, keyReads.get(), "inactive users must never reach an exchange credential lookup");
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class) return false;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (List.class.isAssignableFrom(type)) return List.of();
        return null;
    }
}
