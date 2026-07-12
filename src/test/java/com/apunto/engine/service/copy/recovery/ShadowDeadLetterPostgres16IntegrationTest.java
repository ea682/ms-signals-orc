package com.apunto.engine.service.copy.recovery;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowDeadLetterPostgres16IntegrationTest {

    @Test
    void orderedSingleFlightClaimPlansOnPostgres16WithoutWriting() throws Exception {
        String jdbcUrl = System.getProperty("copy.postgres.test.jdbc-url");
        Assumptions.assumeTrue(jdbcUrl != null && !jdbcUrl.isBlank(),
                "configure copy.postgres.test.jdbc-url for PostgreSQL 16 read-only verification");

        try (Connection connection = DriverManager.getConnection(
                jdbcUrl,
                System.getProperty("copy.postgres.test.username"),
                System.getProperty("copy.postgres.test.password"))) {
            connection.setReadOnly(true);
            try (Statement statement = connection.createStatement();
                 ResultSet version = statement.executeQuery("show server_version_num")) {
                assertTrue(version.next());
                int versionNumber = Integer.parseInt(version.getString(1));
                assertTrue(versionNumber >= 160000 && versionNumber < 170000);
            }

            try (PreparedStatement explain = connection.prepareStatement(
                    "EXPLAIN " + ShadowEventDeadLetterStore.CLAIM_RECOVERABLE_SQL)) {
                explain.setLong(1, 60_000L);
                explain.setInt(2, 50);
                try (ResultSet plan = explain.executeQuery()) {
                    assertTrue(plan.next(), "PostgreSQL must produce a plan for the ordered DLQ claim");
                }
            }
        }
    }
}
