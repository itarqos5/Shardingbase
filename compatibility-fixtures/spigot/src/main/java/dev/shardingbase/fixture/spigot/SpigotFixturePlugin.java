package dev.shardingbase.fixture.spigot;

import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.jspecify.annotations.NullMarked;

/** Legacy Spigot descriptor and event registration compatibility fixture. */
@NullMarked
public final class SpigotFixturePlugin extends JavaPlugin implements Listener {
    @Override
    public void onEnable() {
        this.getServer().getPluginManager().registerEvents(this, this);
        this.getLogger().info("SHARDINGBASE_FIXTURE_SPIGOT_ENABLED");
    }

    @EventHandler
    public void onJoin(final PlayerJoinEvent event) {
        event.getPlayer().sendMessage("Shardingbase legacy Spigot fixture loaded");
    }
}
