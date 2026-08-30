package dev.shardingbase.fixture.paper;

import dev.shardingbase.api.Shardingbase;
import io.papermc.paper.ServerBuildInfo;
import net.kyori.adventure.key.Key;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

/** Paper descriptor fixture which also verifies that the Shardingbase API is available. */
public final class PaperFixturePlugin extends JavaPlugin {
    @Override
    public void onEnable() {
        final ServerBuildInfo build = ServerBuildInfo.buildInfo();
        if (!"Paper".equals(Bukkit.getName())) {
            throw new IllegalStateException("Legacy Bukkit identity must be Paper, got " + Bukkit.getName());
        }
        if (!Bukkit.getVersion().matches("26\\.2-[1-9][0-9]*-[0-9a-f]{7} \\(MC: 26\\.2\\)")) {
            throw new IllegalStateException("Bukkit version is not Paper-compatible: " + Bukkit.getVersion());
        }
        if (!"Shardingbase".equals(build.brandName())) {
            throw new IllegalStateException("Display brand must be Shardingbase, got " + build.brandName());
        }
        if (!build.isBrandCompatible(Key.key("papermc", "paper"))) {
            throw new IllegalStateException("Shardingbase must declare Paper brand compatibility");
        }
        this.getLogger().info(
            "SHARDINGBASE_FIXTURE_PAPER_ENABLED state=" + Shardingbase.service().featureState()
                + " paper=" + Bukkit.getVersion() + " brand=" + build.brandName()
        );
    }
}
