package com.apunto.engine.client;

import com.apunto.engine.config.RestClientConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricWalletsInfoClientContractTest {

    private HttpServer server;
    private MetricWalletsInfoClient client;
    private final AtomicReference<String> joyasQuery = new AtomicReference<>("");
    private final AtomicReference<String> guardQuery = new AtomicReference<>("");

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/operaciones/metrica/joyas", exchange -> respond(exchange, joyasQuery));
        server.createContext("/operaciones/metrica/copy-guard/windows", exchange -> respond(exchange, guardQuery));
        server.start();

        RestClientConfig config = new RestClientConfig();
        ClientHttpRequestFactory requestFactory = config.clientHttpRequestFactory(1_000, 1_000);
        RestClient restClient = config.metricWalletRestClient(
                RestClient.builder(),
                requestFactory,
                "http://127.0.0.1:" + server.getAddress().getPort()
        );
        client = config.metricWalletsInfoClient(restClient);
    }

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void v2DiscoveryAndFullUseCanonicalLimitWalletParameter() {
        client.metricStrategySnapshots(17, 30, "summary");

        assertTrue(joyasQuery.get().contains("limitWallet=17"), joyasQuery::get);
        assertFalse(joyasQuery.get().contains("limit=17"), joyasQuery::get);
    }

    @Test
    void v2CopyGuardUsesCanonicalLimitWalletParameter() {
        client.metricStrategyCopyGuardWindows(19, 30, "snapshot", "1d,all");

        assertTrue(guardQuery.get().contains("limitWallet=19"), guardQuery::get);
        assertFalse(guardQuery.get().contains("limit=19"), guardQuery::get);
    }

    @Test
    void v1RollbackKeepsLegacyLimitParameter() {
        client.joyas(11, 30, "summary");

        assertTrue(joyasQuery.get().contains("limit=11"), joyasQuery::get);
        assertFalse(joyasQuery.get().contains("limitWallet=11"), joyasQuery::get);
    }

    private static void respond(HttpExchange exchange, AtomicReference<String> query) throws IOException {
        query.set(exchange.getRequestURI().getRawQuery());
        byte[] body = "[]".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (var output = exchange.getResponseBody()) {
            output.write(body);
        }
    }
}
