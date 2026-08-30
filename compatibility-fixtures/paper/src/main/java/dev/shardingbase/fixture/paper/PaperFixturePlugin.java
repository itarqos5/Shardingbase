package dev.shardingbase.fixture.paper;

import dev.shardingbase.api.Shardingbase;
import org.bukkit.plugin.java.JavaPlugin;

/** Paper descriptor fixture which also verifies that the Shardingbase API is available. */
public final class PaperFixturePlugin extends JavaPlugin {
    @Override
    public void onEnable() {
        this.getLogger().info(
            "SHARDINGBASE_FIXTURE_PAPER_ENABLED state=" + Shardingbase.service().featureState()
        );
    }
}
