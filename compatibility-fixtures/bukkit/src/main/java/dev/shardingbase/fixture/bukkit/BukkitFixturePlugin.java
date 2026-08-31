package dev.shardingbase.fixture.bukkit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.ChunkPopulateEvent;
import org.bukkit.event.world.WorldInitEvent;
import org.bukkit.generator.BlockPopulator;
import org.bukkit.generator.LimitedRegion;
import org.bukkit.generator.WorldInfo;
import org.bukkit.plugin.java.JavaPlugin;
import org.jspecify.annotations.Nullable;

/** Minimal legacy Bukkit loader compatibility fixture. */
public final class BukkitFixturePlugin extends JavaPlugin implements Listener {
    private final Set<UUID> observedWorlds = new HashSet<>();
    private int boundaryCutChunk = Integer.MAX_VALUE;
    private @Nullable Path observations;

    @Override
    public void onEnable() {
        this.getLogger().info("SHARDINGBASE_FIXTURE_BUKKIT_ENABLED");
        final String configuredCut = System.getProperty("shardingbase.boundary-test.cut-chunk");
        if (configuredCut == null) {
            return;
        }
        this.boundaryCutChunk = Integer.parseInt(configuredCut);
        this.observations = this.getDataFolder().toPath().resolve("boundary-observations.txt");
        try {
            Files.createDirectories(this.observations.getParent());
            Files.deleteIfExists(this.observations);
        } catch (final IOException exception) {
            throw new IllegalStateException("Unable to initialize boundary observations", exception);
        }
        this.getServer().getPluginManager().registerEvents(this, this);
        Bukkit.getWorlds().forEach(this::observeWorld);
    }

    @EventHandler
    public void onWorldInit(final WorldInitEvent event) {
        if (this.observations != null) {
            this.observeWorld(event.getWorld());
        }
    }

    @EventHandler
    public void onChunkLoad(final ChunkLoadEvent event) {
        this.observePeerChunk("chunk-load", event.getWorld(), event.getChunk().getX(), event.getChunk().getZ());
    }

    @EventHandler
    public void onChunkPopulate(final ChunkPopulateEvent event) {
        this.observePeerChunk("chunk-populate", event.getWorld(), event.getChunk().getX(), event.getChunk().getZ());
    }

    private synchronized void observeWorld(final World world) {
        if (!this.observedWorlds.add(world.getUID())) {
            return;
        }
        this.append("world=" + world.getKey() + " uuid=" + world.getUID());
        world.getPopulators().add(new BoundaryObserverPopulator(this, world.getUID()));
    }

    private synchronized void observePeerChunk(
        final String event,
        final World world,
        final int chunkX,
        final int chunkZ
    ) {
        if (this.observations != null && chunkX >= this.boundaryCutChunk) {
            this.append(event + " world=" + world.getKey() + " chunk=" + chunkX + ',' + chunkZ);
        }
    }

    private synchronized void observePopulation(final UUID worldId, final int chunkX, final int chunkZ) {
        if (chunkX >= this.boundaryCutChunk) {
            this.append("block-populator world=" + worldId + " chunk=" + chunkX + ',' + chunkZ);
        }
    }

    private void append(final String line) {
        final Path output = this.observations;
        if (output == null) {
            return;
        }
        try {
            Files.writeString(
                output,
                line + System.lineSeparator(),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            );
        } catch (final IOException exception) {
            throw new IllegalStateException("Unable to record boundary observation", exception);
        }
    }

    private static final class BoundaryObserverPopulator extends BlockPopulator {
        private final BukkitFixturePlugin plugin;
        private final UUID worldId;

        private BoundaryObserverPopulator(final BukkitFixturePlugin plugin, final UUID worldId) {
            this.plugin = plugin;
            this.worldId = worldId;
        }

        @Override
        public void populate(
            final WorldInfo worldInfo,
            final Random random,
            final int chunkX,
            final int chunkZ,
            final LimitedRegion limitedRegion
        ) {
            limitedRegion.getTileEntities();
            this.plugin.observePopulation(this.worldId, chunkX, chunkZ);
        }
    }
}
