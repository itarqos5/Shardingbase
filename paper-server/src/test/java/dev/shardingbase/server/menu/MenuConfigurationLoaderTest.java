package dev.shardingbase.server.menu;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.logging.Logger;
import org.bukkit.Material;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MenuConfigurationLoaderTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-30T12:34:56.789Z"), ZoneOffset.UTC);

    @Test
    void createsAllDefaultMenus(@TempDir final Path directory) {
        final Map<String, MenuDefinition> menus = loader(directory).load();

        assertEquals(4, menus.size());
        assertTrue(Files.isRegularFile(directory.resolve("main.yml")));
        assertTrue(Files.isRegularFile(directory.resolve("player-data.yml")));
        assertTrue(Files.isRegularFile(directory.resolve("confirmation.yml")));
        assertTrue(Files.isRegularFile(directory.resolve("world-sharding.yml")));
        assertTrue(menus.get(DefaultMenus.MAIN).buttons().containsKey("player-data"));
        assertTrue(menus.get(DefaultMenus.MAIN).buttons().containsKey("world-sharding"));
    }

    @Test
    void loadsValidCustomMenu(@TempDir final Path directory) throws Exception {
        loader(directory).load();
        Files.writeString(directory.resolve("main.yml"), """
            title: '<gold>Custom'
            rows: 1
            buttons:
              player-data:
                enabled: true
                material: DIAMOND
                slot: 0
                name: '<aqua>Players'
                lore:
                  - '<gray>Portable state'
            """);

        final MenuDefinition menu = loader(directory).load().get(DefaultMenus.MAIN);

        assertEquals("<gold>Custom", menu.title());
        assertEquals(1, menu.rows());
        assertEquals(Material.DIAMOND, menu.buttons().get("player-data").material());
    }

    @Test
    void backsUpAndRepairsInvalidMenuOnly(@TempDir final Path directory) throws Exception {
        loader(directory).load();
        final Path main = directory.resolve("main.yml");
        Files.writeString(main, "title: Broken\nrows: 9\nbuttons: {}\n");
        final String playerDataBefore = Files.readString(directory.resolve("player-data.yml"));

        final Map<String, MenuDefinition> menus = loader(directory).load();

        assertEquals(3, menus.get(DefaultMenus.MAIN).rows());
        assertEquals(playerDataBefore, Files.readString(directory.resolve("player-data.yml")));
        try (var paths = Files.list(directory)) {
            assertTrue(paths.anyMatch(path -> path.getFileName().toString().startsWith("main.yml.backup-")));
        }
        assertFalse(Files.readString(main).contains("rows: 9"));
    }

    @Test
    void duplicateKeysFallBackWithoutAffectingIdentityFiles(@TempDir final Path directory) throws Exception {
        loader(directory).load();
        final Path main = directory.resolve("main.yml");
        Files.writeString(main, "title: One\ntitle: Two\nrows: 1\nbuttons: {}\n");

        final MenuDefinition definition = loader(directory).load().get(DefaultMenus.MAIN);

        assertEquals(DefaultMenus.all().get(DefaultMenus.MAIN), definition);
    }

    private static MenuConfigurationLoader loader(final Path directory) {
        return new MenuConfigurationLoader(directory, Logger.getAnonymousLogger(), CLOCK);
    }
}
