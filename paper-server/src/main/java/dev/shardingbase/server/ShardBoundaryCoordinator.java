package dev.shardingbase.server;

import dev.shardingbase.api.FeatureState;
import dev.shardingbase.api.PeerStatus;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.server.player.PlayerHandoffClient;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.event.ClickEvent;
import org.bukkit.Bukkit;
import org.bukkit.Color;
import org.bukkit.Location;
import org.bukkit.Particle;
import org.bukkit.World;
import org.bukkit.entity.Player;

/** Enforces visible shard boundaries and initiates safe managed player handoffs. */
final class ShardBoundaryCoordinator implements AutoCloseable {
    private static final long TICK_MILLIS = 50;
    private static final long PARTICLE_INTERVAL_TICKS = 10;
    private static final double PARTICLE_DISTANCE = 48.0;
    private static final double APPROACH_DISTANCE = 0.75;
    private static final double MAX_WALK_STEP = 1.5;
    private static final double CROSSING_INSET = 0.5;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(3);
    private static final Particle.DustOptions WALL_DUST = new Particle.DustOptions(Color.RED, 1.0F);

    private final Supplier<ShardManifestRegistry> manifests;
    private final Supplier<FeatureState> featureState;
    private final Supplier<PeerStatus> peerStatus;
    private final Predicate<UUID> managedFrozen;
    private final Consumer<String> integrityFailure;
    private final PlayerHandoffClient handoff;
    private final Logger logger;
    private final ScheduledExecutorService timer;
    private final ThreadPoolExecutor transport;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean tickQueued = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<UUID, Location> lastLocal = new HashMap<>();
    private final Map<UUID, PendingCrossing> pending = new HashMap<>();
    private final Map<UUID, Long> retryAfterNanos = new HashMap<>();
    private final Map<UUID, UUID> notices = new HashMap<>();
    private final Set<String> reportedIntegrityFailures = new HashSet<>();
    private volatile Executor serverExecutor;
    private long ticks;

    ShardBoundaryCoordinator(
        final String backendId,
        final Supplier<ShardManifestRegistry> manifests,
        final Supplier<FeatureState> featureState,
        final Supplier<PeerStatus> peerStatus,
        final Predicate<UUID> managedFrozen,
        final Consumer<String> integrityFailure,
        final Logger logger
    ) {
        this.manifests = manifests;
        this.featureState = featureState;
        this.peerStatus = peerStatus;
        this.managedFrozen = managedFrozen;
        this.integrityFailure = integrityFailure;
        this.handoff = new PlayerHandoffClient(backendId);
        this.logger = logger;
        this.timer = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Boundary Clock")
            .unstarted(task));
        this.transport = new ThreadPoolExecutor(
            1,
            1,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32),
            task -> Thread.ofPlatform().daemon(true).name("Shardingbase Boundary Transport").unstarted(task),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    void start(final Executor serverExecutor) {
        this.serverExecutor = java.util.Objects.requireNonNull(serverExecutor, "serverExecutor");
        if (this.started.compareAndSet(false, true)) {
            this.timer.scheduleAtFixedRate(this::queueTick, TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    boolean frozen(final UUID playerId) {
        return this.pending.containsKey(playerId);
    }

    void disconnected(final UUID playerId) {
        this.pending.remove(playerId);
        this.lastLocal.remove(playerId);
        this.retryAfterNanos.remove(playerId);
        this.notices.remove(playerId);
    }

    ShardingbaseRuntime.PlayerTeleportRouting routeTeleport(final Player player, final Location target) {
        final World world = target.getWorld();
        if (world == null) {
            return ShardingbaseRuntime.PlayerTeleportRouting.REJECTED;
        }
        final String worldKey = world.getKey().toString();
        final ShardManifestRegistry.Boundary boundary = this.manifests.get().boundary(worldKey).orElse(null);
        if (boundary == null || boundary.owns(target.getBlockX(), target.getBlockZ())) {
            return ShardingbaseRuntime.PlayerTeleportRouting.LOCAL;
        }
        if (!boundary.worldId().equals(world.getUID())) {
            final String detail = "world identity mismatch for " + worldKey + ": manifest "
                + boundary.worldId() + ", loaded " + world.getUID();
            if (this.reportedIntegrityFailures.add(detail)) {
                this.logger.severe("Shardingbase ownership lock: " + detail);
                this.integrityFailure.accept(detail);
            }
            player.sendMessage(Component.text("Shardingbase rejected the teleport: " + detail));
            return ShardingbaseRuntime.PlayerTeleportRouting.REJECTED;
        }

        final PeerStatus peer = this.peerStatus.get();
        if (this.featureState.get() != FeatureState.ENABLED || !peer.available()
            || !boundary.peerId().equals(peer.serverId())) {
            player.sendMessage(Component.text("Shardingbase cannot reach the shard that owns that destination."));
            return ShardingbaseRuntime.PlayerTeleportRouting.REJECTED;
        }

        final PlayerHandoffCodec.TransferDestination destination = new PlayerHandoffCodec.TransferDestination(
            worldKey,
            world.getUID(),
            target.getX(),
            target.getY(),
            target.getZ(),
            target.getYaw(),
            target.getPitch()
        );
        final boolean accepted = this.beginTransfer(player, peer.serverId(), destination, player.getLocation());
        return accepted
            ? ShardingbaseRuntime.PlayerTeleportRouting.ACCEPTED
            : ShardingbaseRuntime.PlayerTeleportRouting.REJECTED;
    }

    private void queueTick() {
        final Executor executor = this.serverExecutor;
        if (executor == null || this.closed.get() || !this.tickQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    this.tick();
                } catch (final RuntimeException exception) {
                    this.logger.log(Level.WARNING, "Shardingbase boundary tick failed", exception);
                } finally {
                    this.tickQueued.set(false);
                }
            });
        } catch (final RejectedExecutionException exception) {
            this.tickQueued.set(false);
            if (!this.closed.get()) {
                this.logger.log(Level.FINE, "Shardingbase server executor rejected a boundary tick", exception);
            }
        }
    }

    private void tick() {
        this.ticks++;
        final Set<UUID> online = new HashSet<>();
        for (final Player player : Bukkit.getOnlinePlayers()) {
            online.add(player.getUniqueId());
            this.tick(player);
        }
        this.lastLocal.keySet().removeIf(playerId -> !online.contains(playerId));
        this.pending.keySet().removeIf(playerId -> !online.contains(playerId));
        this.retryAfterNanos.keySet().removeIf(playerId -> !online.contains(playerId));
        this.notices.keySet().removeIf(playerId -> !online.contains(playerId));
    }

    private void tick(final Player player) {
        final String worldKey = player.getWorld().getKey().toString();
        final ShardManifestRegistry.Boundary boundary = this.manifests.get().boundary(worldKey).orElse(null);
        if (boundary == null) {
            this.lastLocal.remove(player.getUniqueId());
            this.pending.remove(player.getUniqueId());
            return;
        }
        if (!boundary.worldId().equals(player.getWorld().getUID())) {
            final String detail = "world identity mismatch for " + worldKey + ": manifest "
                + boundary.worldId() + ", loaded " + player.getWorld().getUID();
            if (this.reportedIntegrityFailures.add(detail)) {
                this.logger.severe("Shardingbase ownership lock: " + detail);
                this.integrityFailure.accept(detail);
            }
            this.returnOrDisconnect(player, this.lastLocal.get(player.getUniqueId()), detail);
            return;
        }

        this.sendOperatorNotice(player, boundary);
        if (this.ticks % PARTICLE_INTERVAL_TICKS == 0) {
            this.showWall(player, boundary);
        }

        final UUID playerId = player.getUniqueId();
        final Location current = player.getLocation();
        final PendingCrossing active = this.pending.get(playerId);
        if (active != null) {
            this.returnOrDisconnect(player, active.safeLocal(), "boundary transfer is pending");
            if (!this.managedFrozen.test(playerId)
                && System.nanoTime() - active.startedNanos() > REQUEST_TIMEOUT.toNanos()) {
                this.release(player, "Boundary transfer timed out; you remain safely on this shard.");
            }
            return;
        }
        if (boundary.owns(current.getBlockX(), current.getBlockZ())) {
            final Location previous = this.lastLocal.get(playerId);
            this.lastLocal.put(playerId, current.clone());
            if (approachingBoundary(boundary, current, previous)) {
                this.beginApproachTransfer(player, boundary, current);
            }
            return;
        }

        final Location safeLocal = this.safeLocal(boundary, current, this.lastLocal.get(playerId));
        this.returnOrDisconnect(player, safeLocal, "crossed the local shard boundary");
        final PeerStatus peer = this.peerStatus.get();
        if (this.featureState.get() != FeatureState.ENABLED || !peer.available()
            || !boundary.peerId().equals(peer.serverId())) {
            this.deferRetry(player, "Peer shard is unavailable; you remain on this shard.");
            return;
        }
        final long now = System.nanoTime();
        if (now < this.retryAfterNanos.getOrDefault(playerId, 0L)) {
            return;
        }
        final PlayerHandoffCodec.TransferDestination destination = new PlayerHandoffCodec.TransferDestination(
            worldKey,
            player.getWorld().getUID(),
            current.getX(),
            current.getY(),
            current.getZ(),
            current.getYaw(),
            current.getPitch()
        );
        this.beginTransfer(player, peer.serverId(), destination, safeLocal);
    }

    private void beginApproachTransfer(
        final Player player,
        final ShardManifestRegistry.Boundary boundary,
        final Location current
    ) {
        final PeerStatus peer = this.peerStatus.get();
        if (this.featureState.get() != FeatureState.ENABLED || !peer.available()
            || !boundary.peerId().equals(peer.serverId())) {
            return;
        }
        final UUID playerId = player.getUniqueId();
        final long now = System.nanoTime();
        if (now < this.retryAfterNanos.getOrDefault(playerId, 0L)) {
            return;
        }
        final Location target = current.clone();
        final double remoteCoordinate = boundary.ownedSide() == ShardManifestRegistry.Side.NEGATIVE
            ? boundary.cutBlock() + CROSSING_INSET
            : boundary.cutBlock() - CROSSING_INSET;
        if (boundary.axis() == ShardManifestRegistry.Axis.X) {
            target.setX(remoteCoordinate);
        } else {
            target.setZ(remoteCoordinate);
        }
        this.beginTransfer(player, peer.serverId(), new PlayerHandoffCodec.TransferDestination(
            target.getWorld().getKey().toString(),
            target.getWorld().getUID(),
            target.getX(),
            target.getY(),
            target.getZ(),
            target.getYaw(),
            target.getPitch()
        ), current);
    }

    static boolean approachingBoundary(
        final ShardManifestRegistry.Boundary boundary,
        final Location current,
        final Location previous
    ) {
        if (previous == null || previous.getWorld() != current.getWorld()) {
            return false;
        }
        final double currentAxis = boundary.axis() == ShardManifestRegistry.Axis.X
            ? current.getX()
            : current.getZ();
        final double previousAxis = boundary.axis() == ShardManifestRegistry.Axis.X
            ? previous.getX()
            : previous.getZ();
        final double step = currentAxis - previousAxis;
        final boolean towardPeer = boundary.ownedSide() == ShardManifestRegistry.Side.NEGATIVE
            ? step > 0.0
            : step < 0.0;
        return towardPeer
            && Math.abs(step) <= MAX_WALK_STEP
            && Math.abs(currentAxis - boundary.cutBlock()) <= APPROACH_DISTANCE
            && Math.abs(previousAxis - boundary.cutBlock()) <= APPROACH_DISTANCE + MAX_WALK_STEP;
    }

    private boolean beginTransfer(
        final Player player,
        final String peerId,
        final PlayerHandoffCodec.TransferDestination destination,
        final Location safeLocal
    ) {
        final UUID playerId = player.getUniqueId();
        final PendingCrossing crossing = new PendingCrossing(safeLocal.clone(), System.nanoTime());
        if (this.pending.putIfAbsent(playerId, crossing) != null) {
            player.sendActionBar(Component.text("A Shardingbase transfer is already pending."));
            return false;
        }
        player.sendActionBar(Component.text("Transferring to the peer shard…"));
        try {
            this.transport.execute(() -> this.requestTransfer(playerId, peerId, destination, crossing));
            return true;
        } catch (final RejectedExecutionException exception) {
            this.pending.remove(playerId, crossing);
            this.deferRetry(player, "Boundary transfer queue is busy; you remain on this shard.");
            return false;
        }
    }

    private void requestTransfer(
        final UUID playerId,
        final String peerId,
        final PlayerHandoffCodec.TransferDestination destination,
        final PendingCrossing crossing
    ) {
        try {
            final PlayerHandoffCodec.BoundaryResponse response = this.handoff.boundary(peerId, playerId, destination);
            if (!response.accepted()) {
                this.scheduleRelease(playerId, crossing, "Boundary transfer rejected: " + response.detail());
            }
        } catch (final IOException exception) {
            this.scheduleRelease(playerId, crossing, "Boundary transfer unavailable: " + exception.getMessage());
        }
    }

    private void scheduleRelease(final UUID playerId, final PendingCrossing crossing, final String detail) {
        final Executor executor = this.serverExecutor;
        if (executor == null || this.closed.get()) {
            return;
        }
        executor.execute(() -> {
            if (this.pending.remove(playerId, crossing)) {
                this.retryAfterNanos.put(playerId, System.nanoTime() + RETRY_DELAY.toNanos());
                final Player player = Bukkit.getPlayer(playerId);
                if (player != null && player.isOnline()) {
                    player.sendMessage(Component.text(detail));
                }
            }
        });
    }

    private void release(final Player player, final String detail) {
        this.pending.remove(player.getUniqueId());
        this.retryAfterNanos.put(player.getUniqueId(), System.nanoTime() + RETRY_DELAY.toNanos());
        player.sendMessage(Component.text(detail));
    }

    private void deferRetry(final Player player, final String detail) {
        final UUID playerId = player.getUniqueId();
        final long now = System.nanoTime();
        if (now >= this.retryAfterNanos.getOrDefault(playerId, 0L)) {
            player.sendActionBar(Component.text(detail));
            this.retryAfterNanos.put(playerId, now + RETRY_DELAY.toNanos());
        }
    }

    private static Location safeLocal(
        final ShardManifestRegistry.Boundary boundary,
        final Location attempted,
        final Location remembered
    ) {
        if (remembered != null && remembered.getWorld() == attempted.getWorld()
            && boundary.owns(remembered.getBlockX(), remembered.getBlockZ())) {
            return remembered.clone();
        }
        final Location safe = attempted.clone();
        final double coordinate = boundary.ownedSide() == ShardManifestRegistry.Side.NEGATIVE
            ? boundary.cutBlock() - 0.5
            : boundary.cutBlock() + 0.5;
        if (boundary.axis() == ShardManifestRegistry.Axis.X) {
            safe.setX(coordinate);
        } else {
            safe.setZ(coordinate);
        }
        return safe;
    }

    private static void returnOrDisconnect(final Player player, final Location safe, final String reason) {
        if (safe == null || !player.teleport(safe)) {
            player.kick(Component.text("Shardingbase stopped an unsafe shard crossing: " + reason));
        }
    }

    private void sendOperatorNotice(final Player player, final ShardManifestRegistry.Boundary boundary) {
        if (!player.isOp() || boundary.transactionId().equals(this.notices.get(player.getUniqueId()))) {
            return;
        }
        this.notices.put(player.getUniqueId(), boundary.transactionId());
        player.sendMessage(Component.text(
            "Shardingbase shard: " + boundary.ownedSide().name().toLowerCase(java.util.Locale.ROOT)
                + ' ' + boundary.axis() + " side, cut chunk " + boundary.cutChunk()
                + ", peer " + boundary.peerId() + ". "
        ).append(Component.text("[Open status]").clickEvent(ClickEvent.runCommand("/shardingbase"))));
    }

    private void showWall(final Player player, final ShardManifestRegistry.Boundary boundary) {
        final Location location = player.getLocation();
        final double axisPosition = boundary.axis() == ShardManifestRegistry.Axis.X
            ? location.getX()
            : location.getZ();
        if (Math.abs(axisPosition - boundary.cutBlock()) > PARTICLE_DISTANCE) {
            return;
        }
        for (int vertical = -8; vertical <= 8; vertical += 2) {
            for (int horizontal = -16; horizontal <= 16; horizontal += 2) {
                final double x = boundary.axis() == ShardManifestRegistry.Axis.X
                    ? boundary.cutBlock()
                    : location.getX() + horizontal;
                final double z = boundary.axis() == ShardManifestRegistry.Axis.Z
                    ? boundary.cutBlock()
                    : location.getZ() + horizontal;
                player.spawnParticle(Particle.DUST, x, location.getY() + vertical, z, 1, WALL_DUST);
            }
        }
    }

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        this.timer.shutdownNow();
        this.transport.shutdownNow();
        this.pending.clear();
    }

    private record PendingCrossing(Location safeLocal, long startedNanos) {
    }
}
