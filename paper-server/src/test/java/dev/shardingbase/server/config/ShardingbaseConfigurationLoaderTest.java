package dev.shardingbase.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShardingbaseConfigurationLoaderTest {
    private static final UUID GENERATED_ID = UUID.fromString("5d9d985f-7709-4fca-ad2b-4c742cbb3681");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-30T12:34:56.789Z"), ZoneOffset.UTC);

    @Test
    void createsMissingConfigurationWithExactShape(@TempDir final Path directory) throws Exception {
        final Path configurationPath = directory.resolve("config").resolve("shardingbase.yml");
        final ShardingbaseConfiguration configuration = loader(configurationPath).load();

        assertEquals(GENERATED_ID.toString(), configuration.identity().serverId());
        assertEquals("change-me", configuration.identity().serverName());
        assertEquals(
            "server-id: \"" + GENERATED_ID + "\"\nserver-name: \"change-me\"\n",
            Files.readString(configurationPath, StandardCharsets.UTF_8)
        );
        assertTrue(backups(configurationPath).isEmpty());
    }

    @Test
    void coercesScalarsAndBacksUpBeforeRepair(@TempDir final Path directory) throws Exception {
        final Path configurationPath = directory.resolve("shardingbase.yml");
        final String original = "server-id: 42\nserver-name: true\nextra: removed\n";
        Files.writeString(configurationPath, original, StandardCharsets.UTF_8);

        final ShardingbaseConfiguration configuration = loader(configurationPath).load();

        assertEquals("42", configuration.identity().serverId());
        assertEquals("true", configuration.identity().serverName());
        assertEquals("server-id: \"42\"\nserver-name: \"true\"\n", Files.readString(configurationPath));
        final List<Path> backups = backups(configurationPath);
        assertEquals(1, backups.size());
        assertEquals(original, Files.readString(backups.getFirst()));
    }

    @Test
    void repairsEmptyFileAndBlankId(@TempDir final Path directory) throws Exception {
        final Path empty = directory.resolve("empty.yml");
        Files.writeString(empty, "");
        assertEquals(GENERATED_ID.toString(), loader(empty).load().identity().serverId());

        final Path blank = directory.resolve("blank.yml");
        Files.writeString(blank, "server-id: \" \"\nserver-name: backend-a\n");
        assertEquals(GENERATED_ID.toString(), loader(blank).load().identity().serverId());
    }

    @Test
    void rejectsDuplicateKeysWithoutChangingFile(@TempDir final Path directory) throws Exception {
        final Path configurationPath = directory.resolve("shardingbase.yml");
        final String original = "server-id: one\nserver-id: two\nserver-name: backend-a\n";
        Files.writeString(configurationPath, original);

        assertThrows(ShardingbaseConfigurationException.class, () -> loader(configurationPath).load());
        assertEquals(original, Files.readString(configurationPath));
        assertTrue(backups(configurationPath).isEmpty());
    }

    @Test
    void rejectsStructuredIdentityValues(@TempDir final Path directory) throws Exception {
        final Path configurationPath = directory.resolve("shardingbase.yml");
        Files.writeString(configurationPath, "server-id: [one, two]\nserver-name: backend-a\n");

        assertThrows(ShardingbaseConfigurationException.class, () -> loader(configurationPath).load());
        assertFalse(Files.readString(configurationPath).isBlank());
    }

    private static ShardingbaseConfigurationLoader loader(final Path path) {
        return new ShardingbaseConfigurationLoader(path, CLOCK, () -> GENERATED_ID);
    }

    private static List<Path> backups(final Path configurationPath) throws IOException {
        final Path parent = configurationPath.getParent();
        if (Files.notExists(parent)) {
            return List.of();
        }
        try (var paths = Files.list(parent)) {
            final String prefix = configurationPath.getFileName() + ".backup-";
            return paths.filter(path -> path.getFileName().toString().startsWith(prefix)).toList();
        }
    }
}
