package dev.shardingbase.server.map;

import dev.shardingbase.protocol.MapPlannerCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.server.validation.LocalNodeClient;
import dev.shardingbase.world.GeneratedChunkIndex;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import javax.imageio.ImageIO;
import org.bukkit.Chunk;
import org.bukkit.ChunkSnapshot;
import org.bukkit.World;

/** Incrementally renders generated terrain into bounded top-down PNG tiles without generating chunks. */
public final class WorldMapCoordinator implements AutoCloseable {
    private final String backendId;
    private final Logger logger;
    private final LocalNodeClient node = new LocalNodeClient();
    private final ExecutorService io = Executors.newFixedThreadPool(2, task -> Thread.ofPlatform()
        .daemon(true)
        .name("Shardingbase World Map")
        .unstarted(task));
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Executor serverExecutor;

    public WorldMapCoordinator(final String backendId, final Logger logger) {
        this.backendId = backendId;
        this.logger = logger;
    }

    public void start(final Executor serverExecutor) {
        this.serverExecutor = java.util.Objects.requireNonNull(serverExecutor, "serverExecutor");
    }

    public CompletableFuture<MapPlannerCodec.Link> create(final World world) {
        if (this.closed.get() || this.serverExecutor == null) {
            return CompletableFuture.failedFuture(new IOException("World map coordinator is unavailable"));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                return GeneratedChunkIndex.scan(world.getWorldFolder().toPath().resolve("region"));
            } catch (final IOException exception) {
                throw new CompletionException(exception);
            }
        }, this.io).thenCompose(scan -> this.open(world, scan));
    }

    private CompletableFuture<MapPlannerCodec.Link> open(final World world, final GeneratedChunkIndex.Scan scan) {
        final UUID sessionId = UUID.randomUUID();
        return CompletableFuture.supplyAsync(() -> {
            try {
                final ProtocolFrame response = this.node.request(
                    this.backendId,
                    ProtocolChannel.MAP,
                    MessageType.MAP_SESSION_CREATE,
                    "velocity",
                    MapPlannerCodec.encodeCreate(new MapPlannerCodec.Create(
                        sessionId,
                        this.backendId,
                        world.getKey().toString(),
                        world.getWorldFolder().getName(),
                        world.getUID(),
                        world.getSeed(),
                        org.bukkit.Bukkit.getUnsafe().getDataVersion(),
                        scan.minChunkX(),
                        scan.maxChunkX(),
                        scan.minChunkZ(),
                        scan.maxChunkZ(),
                        scan.chunks().size(),
                        scan.estimatedBytes()
                    ))
                );
                if (response.messageType() != MessageType.MAP_SESSION_CREATED) {
                    throw new IOException("Velocity rejected the map session");
                }
                final MapPlannerCodec.Created created = MapPlannerCodec.decodeCreated(response.payload());
                if (!created.accepted()) {
                    throw new IOException(created.detail());
                }
                return grouped(scan.chunks());
            } catch (final IOException exception) {
                throw new CompletionException(exception);
            }
        }, this.io).thenCompose(tiles -> this.renderTiles(world, sessionId, new ArrayList<>(tiles.entrySet()), 0))
            .thenCompose(ignored -> CompletableFuture.supplyAsync(() -> this.complete(sessionId), this.io));
    }

    private CompletableFuture<Void> renderTiles(
        final World world,
        final UUID sessionId,
        final List<Map.Entry<TileCoordinate, List<GeneratedChunkIndex.Chunk>>> tiles,
        final int index
    ) {
        if (index >= tiles.size()) {
            return CompletableFuture.completedFuture(null);
        }
        final Map.Entry<TileCoordinate, List<GeneratedChunkIndex.Chunk>> tile = tiles.get(index);
        return this.loadSnapshots(world, tile.getValue(), 0, new ArrayList<>())
            .thenCompose(snapshots -> CompletableFuture.runAsync(() -> {
                try {
                    this.publishTile(sessionId, tile.getKey(), snapshots);
                } catch (final IOException exception) {
                    throw new CompletionException(exception);
                }
            }, this.io))
            .thenCompose(ignored -> this.renderTiles(world, sessionId, tiles, index + 1));
    }

    private CompletableFuture<List<ChunkSnapshot>> loadSnapshots(
        final World world,
        final List<GeneratedChunkIndex.Chunk> chunks,
        final int index,
        final List<ChunkSnapshot> snapshots
    ) {
        if (index >= chunks.size()) {
            return CompletableFuture.completedFuture(List.copyOf(snapshots));
        }
        final GeneratedChunkIndex.Chunk coordinate = chunks.get(index);
        final CompletableFuture<Chunk> loaded = new CompletableFuture<>();
        this.serverExecutor.execute(() -> world.getChunkAtAsync(coordinate.x(), coordinate.z(), false)
            .whenComplete((chunk, failure) -> {
                if (failure != null) {
                    loaded.completeExceptionally(failure);
                } else {
                    loaded.complete(chunk);
                }
            }));
        return loaded.thenCompose(chunk -> {
            if (chunk != null) {
                snapshots.add(chunk.getChunkSnapshot(true, false, true));
            }
            return this.loadSnapshots(world, chunks, index + 1, snapshots);
        });
    }

    private void publishTile(
        final UUID sessionId,
        final TileCoordinate coordinate,
        final List<ChunkSnapshot> snapshots
    ) throws IOException {
        final BufferedImage image = new BufferedImage(
            MapPlannerCodec.TILE_BLOCKS, MapPlannerCodec.TILE_BLOCKS, BufferedImage.TYPE_INT_ARGB
        );
        for (final ChunkSnapshot snapshot : snapshots) {
            final int offsetX = Math.floorMod(snapshot.getX(), MapPlannerCodec.TILE_CHUNKS) * 16;
            final int offsetZ = Math.floorMod(snapshot.getZ(), MapPlannerCodec.TILE_CHUNKS) * 16;
            for (int localZ = 0; localZ < 16; localZ++) {
                for (int localX = 0; localX < 16; localX++) {
                    final int highest = snapshot.getHighestBlockYAt(localX, localZ);
                    final int rgb = 0xFF000000
                        | snapshot.getBlockData(localX, highest, localZ).getMapColor().asRGB();
                    image.setRGB(offsetX + localX, offsetZ + localZ, rgb);
                }
            }
        }
        final ByteArrayOutputStream png = new ByteArrayOutputStream();
        if (!ImageIO.write(image, "PNG", png)) {
            throw new IOException("PNG encoder is unavailable");
        }
        final ProtocolFrame response = this.node.request(
            this.backendId,
            ProtocolChannel.MAP,
            MessageType.MAP_TILE_PUT,
            "velocity",
            MapPlannerCodec.encodeTile(new MapPlannerCodec.Tile(
                sessionId, coordinate.x(), coordinate.z(), png.toByteArray()
            ))
        );
        if (response.messageType() != MessageType.MAP_TILE_ACK) {
            throw new IOException("Velocity rejected a generated map tile");
        }
    }

    private MapPlannerCodec.Link complete(final UUID sessionId) {
        try {
            final ProtocolFrame response = this.node.request(
                this.backendId,
                ProtocolChannel.MAP,
                MessageType.MAP_SESSION_COMPLETE,
                "velocity",
                MapPlannerCodec.encodeSessionId(sessionId)
            );
            if (response.messageType() != MessageType.MAP_PLANNER_LINK) {
                throw new IOException("Velocity did not return a planner link");
            }
            return MapPlannerCodec.decodeLink(response.payload());
        } catch (final IOException exception) {
            throw new CompletionException(exception);
        }
    }

    private static Map<TileCoordinate, List<GeneratedChunkIndex.Chunk>> grouped(
        final List<GeneratedChunkIndex.Chunk> chunks
    ) {
        final Map<TileCoordinate, List<GeneratedChunkIndex.Chunk>> grouped = new LinkedHashMap<>();
        for (final GeneratedChunkIndex.Chunk chunk : chunks) {
            grouped.computeIfAbsent(new TileCoordinate(
                Math.floorDiv(chunk.x(), MapPlannerCodec.TILE_CHUNKS),
                Math.floorDiv(chunk.z(), MapPlannerCodec.TILE_CHUNKS)
            ), ignored -> new ArrayList<>()).add(chunk);
        }
        return grouped;
    }

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.logger.fine("Closing Shardingbase world map renderer");
            this.io.shutdownNow();
        }
    }

    private record TileCoordinate(int x, int z) {
    }
}
