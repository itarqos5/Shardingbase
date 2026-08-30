package dev.shardingbase.server.menu;

import dev.shardingbase.api.FeatureState;
import dev.shardingbase.server.ShardingbaseRuntime;
import dev.shardingbase.protocol.PlayerDataCategory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import org.bukkit.Server;
import org.bukkit.entity.Player;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryDragEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;

/** Creates and authorizes Shardingbase inventory menus. */
public final class ShardingbaseMenuManager {
    private static final MiniMessage MINI_MESSAGE = MiniMessage.miniMessage();

    private final Server server;
    private final ShardingbaseRuntime runtime;
    private final Executor serverExecutor;
    private final MenuConfigurationLoader loader;
    private final AtomicReference<Map<String, MenuDefinition>> definitions;

    public ShardingbaseMenuManager(
        final Server server,
        final ShardingbaseRuntime runtime,
        final Executor serverExecutor,
        final Path serverDirectory,
        final Logger logger
    ) {
        this.server = server;
        this.runtime = runtime;
        this.serverExecutor = serverExecutor;
        this.loader = new MenuConfigurationLoader(serverDirectory.resolve("config").resolve("shardingbase_menus"), logger);
        this.definitions = new AtomicReference<>(this.loader.load());
    }

    /** Atomically publishes a newly loaded set of menu definitions. */
    public void reload() {
        this.definitions.set(this.loader.load());
    }

    /** Opens the main Shardingbase menu for an authorized player. */
    public void openMain(final Player player) {
        if (!player.hasPermission("shardingbase.sync")) {
            player.sendMessage(Component.text("You do not have permission to use the Shardingbase menu."));
            return;
        }
        this.open(player, DefaultMenus.MAIN);
    }

    private void open(final Player player, final String menuId) {
        final MenuDefinition definition = this.definitions.get().get(menuId);
        if (definition == null) {
            player.sendMessage(Component.text("That Shardingbase menu is unavailable."));
            return;
        }
        final ShardingbaseMenuHolder holder = new ShardingbaseMenuHolder(this, menuId);
        final Inventory inventory = this.server.createInventory(
            holder,
            definition.rows() * 9,
            MINI_MESSAGE.deserialize(definition.title())
        );
        holder.inventory(inventory);
        for (final MenuButton button : definition.buttons().values()) {
            if (!button.enabled()) {
                continue;
            }
            final ItemStack item = new ItemStack(button.material());
            final ItemMeta metadata = item.getItemMeta();
            metadata.displayName(MINI_MESSAGE.deserialize(button.name()));
            final ArrayList<Component> lore = new ArrayList<>(button.lore().size());
            for (final String line : button.lore()) {
                lore.add(MINI_MESSAGE.deserialize(line));
            }
            final Set<PlayerDataCategory> categories = categories(button.id());
            if (DefaultMenus.PLAYER_DATA.equals(menuId) && !categories.isEmpty()) {
                final boolean enabled = this.runtime.playerDataCategories().containsAll(categories);
                lore.add(MINI_MESSAGE.deserialize(enabled ? "<green>Enabled" : "<red>Disabled"));
            }
            metadata.lore(lore);
            item.setItemMeta(metadata);
            inventory.setItem(button.slot(), item);
        }
        player.openInventory(inventory);
    }

    /** Cancels and dispatches a click in a Shardingbase-owned view. */
    public static void handleClick(final InventoryClickEvent event) {
        if (!(event.getView().getTopInventory().getHolder() instanceof final ShardingbaseMenuHolder holder)) {
            return;
        }
        event.setCancelled(true);
        if (!(event.getWhoClicked() instanceof final Player player)
            || event.getClickedInventory() != event.getView().getTopInventory()) {
            return;
        }
        holder.manager().dispatch(player, holder.menuId(), event.getRawSlot());
    }

    /** Cancels all drag transactions touching a Shardingbase-owned view. */
    public static void handleDrag(final InventoryDragEvent event) {
        if (event.getView().getTopInventory().getHolder() instanceof ShardingbaseMenuHolder) {
            event.setCancelled(true);
        }
    }

    private void dispatch(final Player player, final String menuId, final int rawSlot) {
        if (!player.hasPermission("shardingbase.sync")) {
            player.closeInventory();
            return;
        }
        final MenuDefinition definition = this.definitions.get().get(menuId);
        if (definition == null) {
            return;
        }
        MenuButton selected = null;
        for (final MenuButton button : definition.buttons().values()) {
            if (button.enabled() && button.slot() == rawSlot) {
                selected = button;
                break;
            }
        }
        if (selected == null) {
            return;
        }
        final String buttonId = selected.id();
        this.serverExecutor.execute(() -> this.activate(player, menuId, buttonId));
    }

    private void activate(final Player player, final String menuId, final String buttonId) {
        if (!player.isOnline()) {
            return;
        }
        if (DefaultMenus.MAIN.equals(menuId)) {
            if ("player-data".equals(buttonId)) {
                this.open(player, DefaultMenus.PLAYER_DATA);
            } else if ("world-sharding".equals(buttonId)) {
                this.open(player, DefaultMenus.WORLD_SHARDING);
            }
            return;
        }
        if ("back".equals(buttonId) || "cancel".equals(buttonId)) {
            this.open(player, DefaultMenus.MAIN);
            return;
        }
        if (DefaultMenus.PLAYER_DATA.equals(menuId) && "sync-all".equals(buttonId)) {
            this.open(player, DefaultMenus.CONFIRMATION);
            return;
        }
        if (DefaultMenus.PLAYER_DATA.equals(menuId)) {
            final Set<PlayerDataCategory> categories = categories(buttonId);
            if (!categories.isEmpty()) {
                this.runtime.togglePlayerDataCategories(categories).whenComplete((selected, failure) ->
                    this.serverExecutor.execute(() -> {
                        if (!player.isOnline()) {
                            return;
                        }
                        if (failure != null) {
                            player.sendMessage(Component.text("Unable to update player synchronization: "
                                + safeMessage(failure)));
                        }
                        this.open(player, DefaultMenus.PLAYER_DATA);
                    })
                );
                return;
            }
        }
        if (DefaultMenus.CONFIRMATION.equals(menuId) && "confirm".equals(buttonId)) {
            player.closeInventory();
            player.sendMessage(Component.text(this.runtime.featureState() == FeatureState.ENABLED
                ? "Player synchronization request submitted."
                : "Player synchronization is unavailable: " + this.runtime.statusDetail()));
            return;
        }
        player.sendMessage(Component.text(this.runtime.featureState() == FeatureState.ENABLED
            ? "This operation is ready for its proxy transaction."
            : "Distributed features are unavailable: " + this.runtime.statusDetail()));
    }

    private static Set<PlayerDataCategory> categories(final String buttonId) {
        return switch (buttonId) {
            case "inventory" -> Set.of(PlayerDataCategory.INVENTORY);
            case "armor" -> Set.of(PlayerDataCategory.ARMOR);
            case "offhand" -> Set.of(PlayerDataCategory.OFFHAND);
            case "selected-slot" -> Set.of(PlayerDataCategory.SELECTED_SLOT);
            case "ender-chest" -> Set.of(PlayerDataCategory.ENDER_CHEST);
            case "experience" -> Set.of(PlayerDataCategory.EXPERIENCE);
            case "health" -> Set.of(PlayerDataCategory.HEALTH);
            case "hunger" -> Set.of(PlayerDataCategory.FOOD);
            case "potion-effects" -> Set.of(PlayerDataCategory.POTION_EFFECTS);
            case "game-mode" -> Set.of(PlayerDataCategory.GAME_MODE, PlayerDataCategory.ABILITIES);
            case "advancements" -> Set.of(PlayerDataCategory.ADVANCEMENTS);
            case "statistics" -> Set.of(PlayerDataCategory.STATISTICS);
            default -> Set.of();
        };
    }

    private static String safeMessage(final Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }
}
