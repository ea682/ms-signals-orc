package com.apunto.copytarget.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyPolicyV1NdjsonRunnerTest {

    private final CopyPolicyV1NdjsonRunner runner = new CopyPolicyV1NdjsonRunner();
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void processesACanonicalStepWithoutStartingAnotherProcess() throws Exception {
        JsonNode response = mapper.readTree(runner.processLine(request("EXPLICIT", "50")));

        assertTrue(response.path("ok").asBoolean());
        assertEquals("step-1", response.path("requestId").asText());
        assertEquals("81", response.path("result").path("authorizedSizingCapitalUsd").asText());
        assertEquals("20.25", response.path("result").path("portfolio")
                .path("totalTargetNotionalUsd").asText());
        assertEquals(64, response.path("result").path("inputDigest").asText().length());
    }

    @Test
    void unavailableMarginReturnsAStableBlockedDecisionInsteadOfZeroSizing() throws Exception {
        JsonNode response = mapper.readTree(runner.processLine(request("UNAVAILABLE", null)));

        assertTrue(response.path("ok").asBoolean());
        assertEquals("BLOCKED_SOURCE_MARGIN_UNAVAILABLE", response.path("result")
                .path("portfolio").path("portfolioDecisionCode").asText());
        assertFalse(response.path("result").path("portfolio").path("entrySizingAllowed").asBoolean());
    }

    @Test
    void invalidProtocolLineFailsClosedAndKeepsTheStreamAlive() throws Exception {
        JsonNode response = mapper.readTree(runner.processLine("{\"requestId\":\"bad\"}"));

        assertFalse(response.path("ok").asBoolean());
        assertEquals("COPY_POLICY_V1_INPUT_REJECTED", response.path("errorCode").asText());
    }

    private String request(String provenance, String margin) {
        String marginJson = margin == null ? "null" : "\"" + margin + "\"";
        return """
                {
                  "requestId":"step-1",
                  "wallet":"wallet-1",
                  "strategyKey":"MOVEMENT_ALL",
                  "world":"CORE",
                  "executionMode":"HISTORICAL",
                  "initialAccountEquityUsd":"100",
                  "currentMtmEquityUsd":"100",
                  "calculatedAt":"2026-08-28T12:00:00Z",
                  "sourceAccountEquityUsd":"1000",
                  "equityObservedAt":"2026-08-28T11:59:59Z",
                  "equitySource":"CERTIFIED_ETL",
                  "maximumEquityAgeMs":30000,
                  "sourceSnapshotVersion":42,
                  "sourcePositions":[{
                    "sourceLegId":"leg-1","sourceSymbol":"BTC","targetSymbol":"BTCUSDT",
                    "side":"LONG","quantity":"0.01","notionalUsd":"500",
                    "marginUsedUsd":%s,"marginProvenance":"%s",
                    "markPrice":"50000","entryPrice":"50000","leverage":"10",
                    "snapshotVersion":42,"liquidityScore":"100"
                  }],
                  "targetLeverage":"5",
                  "availableMarginUsd":"999",
                  "usedMarginUsd":"0",
                  "existingPositions":[],"managedExistingPositions":[],"portfolioExistingPositions":[],
                  "targetPositionSnapshotStatus":"AUTHORITATIVE",
                  "filters":[{
                    "symbol":"BTCUSDT","trading":true,"quoteAsset":"USDT",
                    "minQty":"0.00000001","maxQty":"1000","stepSize":"0.00000001",
                    "minNotional":"1","tickSize":"0.01","maximumLeverage":"20",
                    "liquidityScore":"100"
                  }],
                  "quoteAsset":"USDT",
                  "strategyVersion":"strategy-v1",
                  "sizingPolicyVersion":"copy-policy-v1",
                  "symbolMappingVersion":"binance-rules-v1"
                }
                """.formatted(marginJson, provenance);
    }
}
