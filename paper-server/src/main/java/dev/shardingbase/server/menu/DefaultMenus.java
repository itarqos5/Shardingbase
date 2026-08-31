package dev.shardingbase.server.menu;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bukkit.Material;

/** Built-in menu layouts used for first run and invalid-menu fallback. */
final class DefaultMenus {
    static final String MAIN = "main";
    static final String PLAYER_DATA = "player-data";
    static final String CONFIRMATION = "confirmation";
    static final String WORLD_SHARDING = "world-sharding";

    private DefaultMenus() {
    }

    static Map<String, MenuDefinition> all() {
        final Map<String, MenuDefinition> menus = new LinkedHashMap<>();
        menus.put(MAIN, menu(MAIN, "<dark_aqua>Shardingbase", 3,
            button("player-data", Material.CHEST, 11, "<aqua>Playerdata", "Configure portable player synchronization"),
            button("world-sharding", Material.FILLED_MAP, 15, "<red>World Sharding", "Map and plan an offline world cut")
        ));
        menus.put(PLAYER_DATA, menu(PLAYER_DATA, "<dark_aqua>Playerdata", 6,
            button("inventory", Material.CHEST, 10, "<green>Inventory", "Inventory contents"),
            button("armor", Material.IRON_CHESTPLATE, 11, "<green>Armor", "Equipped armor slots"),
            button("offhand", Material.SHIELD, 12, "<green>Offhand", "Offhand item"),
            button("selected-slot", Material.COMPASS, 13, "<green>Selected slot", "Selected hotbar slot"),
            button("ender-chest", Material.ENDER_CHEST, 14, "<green>Ender chest", "Ender chest contents"),
            button("experience", Material.EXPERIENCE_BOTTLE, 15, "<green>Experience", "Level and experience progress"),
            button("health", Material.GOLDEN_APPLE, 16, "<green>Health", "Health and absorption"),
            button("hunger", Material.COOKED_BEEF, 19, "<green>Hunger", "Food, saturation, and exhaustion"),
            button("potion-effects", Material.POTION, 20, "<green>Potion effects", "Active effects"),
            button("game-mode", Material.DIAMOND_SWORD, 21, "<green>Game mode", "Game mode and abilities"),
            button("advancements", Material.KNOWLEDGE_BOOK, 22, "<green>Advancements", "Advancement progress"),
            button("statistics", Material.WRITABLE_BOOK, 23, "<green>Statistics", "Player statistics"),
            button("sync-all", Material.LIME_DYE, 40, "<yellow>Sync online players", "One-way portable snapshots to the peer"),
            button("back", Material.ARROW, 49, "<gray>Back", "Return to the main menu")
        ));
        menus.put(CONFIRMATION, menu(CONFIRMATION, "<red>Confirm operation", 3,
            button("confirm", Material.LIME_CONCRETE, 11, "<green>Confirm", "Begin the authorized operation"),
            button("cancel", Material.RED_CONCRETE, 15, "<red>Cancel", "Return without making changes")
        ));
        menus.put(WORLD_SHARDING, menu(WORLD_SHARDING, "<red>World Sharding", 3,
            button("select-world", Material.GRASS_BLOCK, 11, "<aqua>Select world", "Choose a loaded world to map"),
            button("open-planner", Material.MAP, 13, "<yellow>Open planner", "Generate a single-use web planner link"),
            button("back", Material.ARROW, 15, "<gray>Back", "Return to the main menu")
        ));
        return Map.copyOf(menus);
    }

    private static MenuDefinition menu(final String id, final String title, final int rows, final MenuButton... buttons) {
        final Map<String, MenuButton> indexed = new LinkedHashMap<>();
        for (final MenuButton button : buttons) {
            indexed.put(button.id(), button);
        }
        return new MenuDefinition(id, title, rows, indexed);
    }

    private static MenuButton button(
        final String id,
        final Material material,
        final int slot,
        final String name,
        final String lore
    ) {
        return new MenuButton(id, true, material, slot, name, List.of("<gray>" + lore));
    }
}
