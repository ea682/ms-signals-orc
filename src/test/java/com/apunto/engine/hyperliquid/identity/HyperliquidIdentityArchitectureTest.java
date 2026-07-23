package com.apunto.engine.hyperliquid.identity;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HyperliquidIdentityArchitectureTest {

    @Test
    void tradeIdempotencyKeysAreBuiltOnlyByOfficialIdentityComponent()
            throws IOException {
        Path sourceRoot = Path.of("src", "main", "java");
        List<Path> violations;
        try (var sources = Files.walk(sourceRoot)) {
            violations = sources
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.getFileName().toString()
                            .equals("HyperliquidSourceTradeIdentity.java"))
                    .filter(this::containsManualTradeKey)
                    .toList();
        }

        assertEquals(
                List.of(),
                violations,
                "manual Hyperliquid trade identity construction is forbidden");
    }

    private boolean containsManualTradeKey(Path path) {
        try {
            return Files.readString(path).contains("hyperliquid:trade:");
        } catch (IOException error) {
            throw new IllegalStateException(error);
        }
    }
}
