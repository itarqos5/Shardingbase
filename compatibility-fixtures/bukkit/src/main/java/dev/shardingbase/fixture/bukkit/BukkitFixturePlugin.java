package dev.shardingbase.fixture.bukkit;

import org.bukkit.plugin.java.JavaPlugin;

/** Minimal legacy Bukkit loader compatibility fixture. */
public final class BukkitFixturePlugin extends JavaPlugin {
    @Override
    public void onEnable() {
        this.getLogger().info("SHARDINGBASE_FIXTURE_BUKKIT_ENABLED");
    }
}
