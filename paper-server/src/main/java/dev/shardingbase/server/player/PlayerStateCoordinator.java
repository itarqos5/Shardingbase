package dev.shardingbase.server.player;

import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.server.validation.LocalNodeClient;
import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bukkit.entity.Player;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import net.kyori.adventure.text.Component;

/** Coordinates non-blocking transport and server-thread capture/application of portable player state. */
public final class PlayerStateCoordinator implements AutoCloseable {
    private static final int QUEUE_CAPACITY = 64;
    private static final EnumSet<PlayerDataCategory> ALL_CATEGORIES = EnumSet.allOf(PlayerDataCategory.class);

    private final String backendId;
    private final PlayerHandoffClient handoff;
    private final PortablePlayerStateAdapter adapter;
    private final AppliedPlayerRevisionStore revisions;
    private final Logger logger;
    private final ThreadPoolExecutor transport;
    private final LocalNodeClient node = new LocalNodeClient();
    private final ConcurrentHashMap<UUID, PlayerHandoffCodec.Capture> managedCaptures = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean polling = new AtomicBoolean();
    private final AtomicReference<Set<PlayerDataCategory>> selectedCategories = new AtomicReference<>(
        Set.copyOf(ALL_CATEGORIES)
    );
    private volatile Executor serverExecutor;
    private volatile Thread pollThread;

    public PlayerStateCoordinator(final String backendId, final Path serverDirectory, final Logger logger) {
        this.backendId = backendId;
        this.handoff = new PlayerHandoffClient(backendId);
        this.adapter = new PortablePlayerStateAdapter();
        this.revisions = new AppliedPlayerRevisionStore(serverDirectory);
        this.logger = logger;
        this.transport = new ThreadPoolExecutor(
            1,
            2,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(QUEUE_CAPACITY),
            task -> Thread.ofPlatform().daemon(true).name("Shardingbase Player Transport").unstarted(task),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    /** Attaches the server-thread executor and starts receiving proxy capture instructions. */
    public void serverExecutor(final Executor serverExecutor) {
        this.serverExecutor = java.util.Objects.requireNonNull(serverExecutor, "serverExecutor");
        if (this.polling.compareAndSet(false, true)) {
            this.pollThread = Thread.ofPlatform()
                .daemon(true)
                .name("Shardingbase Backend Control Poll")
                .start(this::pollLoop);
            this.refreshSettings();
        }
    }

    /** Returns an immutable snapshot of the currently known authority selection. */
    public Set<PlayerDataCategory> categories() {
        return this.selectedCategories.get();
    }

    /** Toggles one or more categories atomically at the Velocity authority. */
    public CompletionStage<Set<PlayerDataCategory>> toggle(final Set<PlayerDataCategory> categories) {
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    final EnumSet<PlayerDataCategory> candidate = EnumSet.copyOf(this.handoff.settings());
                    final boolean allEnabled = candidate.containsAll(categories);
                    if (allEnabled) {
                        if (candidate.size() == categories.size()) {
                            throw new IOException("At least one portable player category must remain enabled");
                        }
                        candidate.removeAll(categories);
                    } else {
                        candidate.addAll(categories);
                    }
                    final Set<PlayerDataCategory> stored = Set.copyOf(this.handoff.settings(candidate));
                    this.selectedCategories.set(stored);
                    return stored;
                } catch (IOException exception) {
                    throw new CompletionException(exception);
                }
            }, this.transport);
        } catch (final RejectedExecutionException exception) {
            return CompletableFuture.failedFuture(new IOException("Player transport queue is full", exception));
        }
    }

    private void refreshSettings() {
        try {
            this.transport.execute(() -> {
                try {
                    this.selectedCategories.set(Set.copyOf(this.handoff.settings()));
                } catch (IOException exception) {
                    this.logger.log(Level.FINE, "Player synchronization settings are not available yet", exception);
                }
            });
        } catch (final RejectedExecutionException _) {
        }
    }

    /** Begins target staging retrieval without blocking the server thread. */
    public CompletionStage<Optional<PlayerHandoffCodec.Stage>> fetch(final UUID playerId) {
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return this.handoff.fetch(playerId);
                } catch (IOException exception) {
                    throw new CompletionException(exception);
                }
            }, this.transport);
        } catch (final RejectedExecutionException exception) {
            return CompletableFuture.failedFuture(new IOException("Player transport queue is full", exception));
        }
    }

    /** Applies a fetched revision on the server thread if it is newer than the durable local ledger. */
    public void applyIfNew(final Player player, final Optional<PlayerHandoffCodec.Stage> fetched) throws IOException {
        if (fetched.isEmpty()) {
            return;
        }
        final PlayerHandoffCodec.Stage stage = fetched.orElseThrow();
        final PlayerSnapshot snapshot = stage.snapshot();
        if (!this.backendId.equals(stage.targetBackendId())) {
            throw new IOException("Player snapshot was staged for a different backend");
        }
        if (!this.revisions.shouldApply(snapshot.playerId(), snapshot.revision())) {
            return;
        }
        this.adapter.apply(player, snapshot);
        if (stage.destination() != null) {
            this.applyDestination(player, stage.destination());
        }
        this.revisions.markApplied(snapshot.playerId(), snapshot.revision());
    }

    private void applyDestination(
        final Player player,
        final PlayerHandoffCodec.TransferDestination destination
    ) throws IOException {
        final World world = Bukkit.getWorld(destination.worldId());
        if (world == null || !world.getKey().toString().equals(destination.worldKey())) {
            throw new IOException("Transfer destination world identity is not loaded on this shard");
        }
        final Location requested = new Location(
            world,
            destination.x(),
            destination.y(),
            destination.z(),
            destination.yaw(),
            destination.pitch()
        );
        final Location safe = safeDestination(requested, 4);
        if (safe == null) {
            throw new IOException("No safe transfer destination exists within four blocks of the crossing");
        }
        if (!samePosition(requested, safe)) {
            this.logger.info("Adjusted Shardingbase transfer destination for " + player.getUniqueId()
                + " from " + format(requested) + " to " + format(safe));
        }
        if (!player.teleport(safe)) {
            throw new IOException("The target server rejected the safe transfer destination");
        }
    }

    static Location safeDestination(final Location requested, final int radius) {
        if (requested.getWorld() == null || radius < 0) {
            throw new IllegalArgumentException("A world and non-negative search radius are required");
        }
        final World world = requested.getWorld();
        final int requestedX = requested.getBlockX();
        final int requestedY = requested.getBlockY();
        final int requestedZ = requested.getBlockZ();
        for (int distance = 0; distance <= radius; distance++) {
            for (int dx = -distance; dx <= distance; dx++) {
                for (int dz = -distance; dz <= distance; dz++) {
                    if (Math.max(Math.abs(dx), Math.abs(dz)) != distance) {
                        continue;
                    }
                    for (int vertical = 0; vertical <= radius; vertical++) {
                        final int positiveY = requestedY + vertical;
                        final Location positive = candidate(requested, world, requestedX + dx, positiveY, requestedZ + dz);
                        if (safe(positive)) {
                            return positive;
                        }
                        if (vertical > 0) {
                            final Location negative = candidate(
                                requested, world, requestedX + dx, requestedY - vertical, requestedZ + dz
                            );
                            if (safe(negative)) {
                                return negative;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private static Location candidate(
        final Location requested,
        final World world,
        final int blockX,
        final int blockY,
        final int blockZ
    ) {
        final boolean exactColumn = blockX == requested.getBlockX() && blockZ == requested.getBlockZ();
        final boolean exactHeight = blockY == requested.getBlockY();
        return new Location(
            world,
            exactColumn ? requested.getX() : blockX + 0.5,
            exactHeight ? requested.getY() : blockY,
            exactColumn ? requested.getZ() : blockZ + 0.5,
            requested.getYaw(),
            requested.getPitch()
        );
    }

    private static boolean safe(final Location location) {
        final World world = location.getWorld();
        if (world == null || location.getBlockY() <= world.getMinHeight()
            || location.getBlockY() + 1 >= world.getMaxHeight()) {
            return false;
        }
        return world.getBlockAt(location).isPassable()
            && world.getBlockAt(location.getBlockX(), location.getBlockY() + 1, location.getBlockZ()).isPassable()
            && world.getBlockAt(location.getBlockX(), location.getBlockY() - 1, location.getBlockZ()).isSolid();
    }

    private static boolean samePosition(final Location first, final Location second) {
        return first.getWorld() == second.getWorld()
            && Double.compare(first.getX(), second.getX()) == 0
            && Double.compare(first.getY(), second.getY()) == 0
            && Double.compare(first.getZ(), second.getZ()) == 0;
    }

    private static String format(final Location location) {
        return location.getWorld().getKey() + " " + location.getX() + "," + location.getY() + "," + location.getZ();
    }

    /**
     * Captures final post-quit state on the server thread and asynchronously replicates it to the peer.
     */
    public void captureAndReplicate(final Player player, final String peerBackendId) {
        final PlayerHandoffCodec.Capture managed = this.managedCaptures.remove(player.getUniqueId());
        final String targetBackendId = managed == null ? peerBackendId : managed.targetBackendId();
        final Set<PlayerDataCategory> categories = managed == null ? this.selectedCategories.get() : managed.categories();
        if (targetBackendId == null || targetBackendId.isBlank()) {
            return;
        }
        final PlayerSnapshot captured;
        try {
            captured = this.adapter.capture(player, managed == null ? 1 : managed.revision(), this.backendId, categories);
        } catch (final IOException | RuntimeException exception) {
            this.logger.log(Level.WARNING, "Unable to capture portable player state for " + player.getUniqueId(), exception);
            return;
        }
        try {
            this.transport.execute(() -> {
                try {
                    final long revision = managed == null
                        ? this.handoff.prepare(captured.playerId(), targetBackendId, categories)
                        : managed.revision();
                    this.handoff.stage(targetBackendId, new PlayerSnapshot(
                        captured.playerId(),
                        revision,
                        this.backendId,
                        captured.categories()
                    ), managed == null ? null : managed.destination());
                } catch (final IOException exception) {
                    this.logger.log(
                        Level.WARNING,
                        "Unable to replicate portable player state for " + captured.playerId(),
                        exception
                    );
                }
            });
        } catch (final RejectedExecutionException exception) {
            this.logger.log(Level.WARNING, "Player transport queue is full; snapshot was not replicated", exception);
        }
    }

    /** Returns whether a managed handoff is currently freezing this player's source state. */
    public boolean frozen(final UUID playerId) {
        return this.managedCaptures.containsKey(playerId);
    }

    private void pollLoop() {
        while (!this.closed.get()) {
            try {
                final ProtocolFrame response = this.node.request(
                    this.backendId,
                    ProtocolChannel.PLAYER_SYNC,
                    MessageType.BACKEND_POLL,
                    "node-local",
                    new byte[0]
                );
                if (response.messageType() == MessageType.PLAYER_SNAPSHOT_CAPTURE) {
                    this.dispatchCapture(PlayerHandoffCodec.decodeCapture(response.payload()));
                } else if (response.messageType() != MessageType.BACKEND_POLL_EMPTY) {
                    this.logger.warning("Ignored unexpected backend control message " + response.messageType());
                }
            } catch (final IOException exception) {
                if (!this.closed.get()) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (final InterruptedException _) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    private void dispatchCapture(final PlayerHandoffCodec.Capture capture) {
        final Executor executor = this.serverExecutor;
        if (executor == null) {
            return;
        }
        executor.execute(() -> {
            final Player player = Bukkit.getPlayer(capture.playerId());
            if (player == null || !player.isOnline()) {
                this.logger.warning("Managed handoff player is no longer online: " + capture.playerId());
                return;
            }
            final PlayerHandoffCodec.Capture existing = this.managedCaptures.putIfAbsent(capture.playerId(), capture);
            if (existing != null && existing.revision() >= capture.revision()) {
                return;
            }
            if (existing != null) {
                this.managedCaptures.put(capture.playerId(), capture);
            }
            player.closeInventory();
            player.kick(Component.text("Shardingbase is transferring you to the peer shard…"));
        });
    }

    @Override
    public void close() {
        this.closed.set(true);
        final Thread currentPollThread = this.pollThread;
        if (currentPollThread != null) {
            currentPollThread.interrupt();
        }
        this.transport.shutdownNow();
    }
}
