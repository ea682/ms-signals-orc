package com.apunto.engine.testsupport;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ProductionBaselinePostgres {

    private ProductionBaselinePostgres() {
    }

    public static MigrateResult restoreAndMigrate(PostgreSQLContainer<?> postgres) throws Exception {
        Path schema = requiredFile("copy.postgres.baseline-schema", "target/audit-schema-baseline.sql");
        Path history = requiredFile("copy.postgres.baseline-history", "target/audit-flyway-history.sql");
        restore(postgres, schema, "/tmp/production-schema.sql");
        restore(postgres, history, "/tmp/production-flyway-history.sql");
        Flyway flyway = flyway(postgres);
        MigrateResult result = flyway.migrate();
        flyway.validate();
        assertEquals(0, flyway.info().pending().length, "all real Flyway migrations must be applied");
        return result;
    }

    public static Flyway flyway(PostgreSQLContainer<?> postgres) {
        return Flyway.configure()
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .schemas("futuros_operaciones")
                .defaultSchema("futuros_operaciones")
                .locations("classpath:db/migration")
                .load();
    }

    private static Path requiredFile(String property, String fallback) {
        Path path = Path.of(System.getProperty(property, fallback)).toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(path),
                () -> "production schema evidence is required for full Flyway integration: " + path);
        return path;
    }

    private static void restore(PostgreSQLContainer<?> postgres, Path hostFile, String containerFile)
            throws Exception {
        postgres.copyFileToContainer(MountableFile.forHostPath(hostFile), containerFile);
        Container.ExecResult restored = postgres.execInContainer(
                "psql", "-v", "ON_ERROR_STOP=1", "-U", postgres.getUsername(),
                "-d", postgres.getDatabaseName(), "-f", containerFile);
        assertEquals(0, restored.getExitCode(), () -> "PostgreSQL restore failed: " + restored.getStderr());
        assertFalse(restored.getStdout().contains("ERROR:"), restored::getStdout);
    }
}
