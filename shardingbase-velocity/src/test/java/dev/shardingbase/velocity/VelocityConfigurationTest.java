package dev.shardingbase.velocity;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VelocityConfigurationTest {
    @Test
    void createsControllerConfigurationWithTwoCredentials(@TempDir final Path directory) throws Exception {
        final VelocityConfiguration configuration = VelocityConfiguration.load(directory);

        assertEquals(2, configuration.nodeCredentials().size());
        assertTrue(configuration.transactionSigningKey().length() >= 43);
        assertEquals(8443, configuration.controlPort());
        assertTrue(configuration.remoteCommandAllowlist().isEmpty());
        assertEquals(8080, configuration.webPort());
        assertTrue(Files.isRegularFile(directory.resolve("config.yml")));
        assertTrue(configuration.keyStorePath().startsWith(directory));
        assertTrue(configuration.databasePath().startsWith(directory));
    }

    @Test
    void rejectsEscapingPaths(@TempDir final Path directory) throws Exception {
        Files.writeString(directory.resolve("config.yml"), """
            control:
              bind: 127.0.0.1
              port: 8443
              keystore: ../outside.p12
              keystore-password: password
            database: shardingbase.db
            node-credentials:
              node-a: credential-a
              node-b: credential-b
            """);

        assertThrows(Exception.class, () -> VelocityConfiguration.load(directory));
    }

    @Test
    void backsUpAndMigratesAnExistingConfigurationWithoutATransactionKey(
        @TempDir final Path directory
    ) throws Exception {
        final Path path = directory.resolve("config.yml");
        Files.writeString(path, """
            control:
              bind: 127.0.0.1
              port: 8443
              keystore: tls.p12
              keystore-password: password
            database: shardingbase.db
            node-credentials:
              node-a: credential-a
              node-b: credential-b
            """);

        final VelocityConfiguration migrated = VelocityConfiguration.load(directory);
        final VelocityConfiguration reloaded = VelocityConfiguration.load(directory);

        assertEquals(migrated.transactionSigningKey(), reloaded.transactionSigningKey());
        assertTrue(Files.readString(path).contains("transaction-signing-key"));
        try (var files = Files.list(directory)) {
            assertEquals(1L, files.filter(file ->
                file.getFileName().toString().startsWith("config.yml.bak.")).count());
        }
    }
}
