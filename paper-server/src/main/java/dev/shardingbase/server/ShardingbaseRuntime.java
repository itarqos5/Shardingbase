package dev.shardingbase.server;

import dev.shardingbase.api.BlockSnapshot;
import dev.shardingbase.api.EntitySpawn;
import dev.shardingbase.api.FeatureState;
import dev.shardingbase.api.Ownership;
import dev.shardingbase.api.PeerStatus;
import dev.shardingbase.api.RemoteOperations;
import dev.shardingbase.api.RemoteResult;
import dev.shardingbase.api.ServerIdentity;
import dev.shardingbase.api.ShardingbaseService;
import dev.shardingbase.api.WorldPosition;
import dev.shardingbase.protocol.MapPlannerCodec;
import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.RemoteOperationCodec;
import dev.shardingbase.server.config.ShardingbaseConfiguration;
import dev.shardingbase.server.config.ShardingbaseConfigurationException;
import dev.shardingbase.server.config.ShardingbaseConfigurationLoader;
import dev.shardingbase.server.map.WorldMapCoordinator;
import dev.shardingbase.server.player.PlayerStateCoordinator;
import dev.shardingbase.server.remote.RemoteCommandCoordinator;
import dev.shardingbase.server.remote.RemoteOperationCoordinator;
import dev.shardingbase.server.validation.BackendValidator;
import dev.shardingbase.server.validation.LocalNodeValidator;
import dev.shardingbase.server.validation.ValidationResult;
import dev.shardingbase.server.world.WorldTransactionCoordinator;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jspecify.annotations.Nullable;

/** Owns the backend identity, feature state, validation retries, and reload lifecycle. */
public final class ShardingbaseRuntime implements ShardingbaseService, AutoCloseable {
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(60);

    private final ShardingbaseConfigurationLoader configurationLoader;
    private final Path serverDirectory;
    private final BackendValidator validator;
    private final Logger logger;
    private final ScheduledExecutorService executor;
    private final AtomicReference<Snapshot> snapshot;
    private final AtomicReference<ShardManifestRegistry> shardManifests;
    private final AtomicLong generation = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final PlayerStateCoordinator playerStateCoordinator;
    private final RemoteOperationCoordinator remoteOperationCoordinator;
    private final RemoteCommandCoordinator remoteCommandCoordinator;
    private final WorldMapCoordinator worldMapCoordinator;
    private final WorldTransactionCoordinator worldTransactionCoordinator;
    private volatile @Nullable ScheduledFuture<?> retryTask;
    private volatile Runnable menuReloader = () -> {
    };
    private final RemoteOperations remoteOperations;

    /**
     * Loads and starts a runtime rooted at the server directory.
     *
     * @param serverDirectory server working directory
     * @param logger server logger
     * @param minecraftVersion running Minecraft version
     * @param shardingbaseVersion running Shardingbase build version
     * @return initialized runtime
     * @throws ShardingbaseConfigurationException if the primary identity configuration is fatally invalid
     */
    public static ShardingbaseRuntime start(
        final Path serverDirectory,
        final Logger logger,
        final String minecraftVersion,
        final String shardingbaseVersion
    )
        throws ShardingbaseConfigurationException {
        final Path configurationPath = serverDirectory.resolve("config").resolve("shardingbase.yml");
        return new ShardingbaseRuntime(
            new ShardingbaseConfigurationLoader(configurationPath),
            new LocalNodeValidator(minecraftVersion, shardingbaseVersion),
            logger,
            createExecutor(),
            serverDirectory.toAbsolutePath().normalize()
        );
    }

    ShardingbaseRuntime(
        final ShardingbaseConfigurationLoader configurationLoader,
        final BackendValidator validator,
        final Logger logger,
        final ScheduledExecutorService executor,
        final Path serverDirectory
    ) throws ShardingbaseConfigurationException {
        this.configurationLoader = configurationLoader;
        this.serverDirectory = serverDirectory;
        this.validator = validator;
        this.logger = logger;
        this.executor = executor;
        final ShardingbaseConfiguration configuration = this.configurationLoader.load();
        this.playerStateCoordinator = new PlayerStateCoordinator(
            configuration.identity().serverId(),
            serverDirectory,
            logger
        );
        this.remoteOperationCoordinator = new RemoteOperationCoordinator(configuration.identity().serverId(), logger);
        this.remoteCommandCoordinator = new RemoteCommandCoordinator(configuration.identity().serverId(), logger);
        this.worldMapCoordinator = new WorldMapCoordinator(configuration.identity().serverId(), logger);
        this.worldTransactionCoordinator =
            new WorldTransactionCoordinator(configuration.identity().serverId(), logger);
        this.remoteOperations = new RoutedRemoteOperations();
        this.shardManifests = new AtomicReference<>(ShardManifestRegistry.load(serverDirectory));
        this.snapshot = new AtomicReference<>(new Snapshot(
            configuration.identity(),
            FeatureState.PENDING,
            "waiting for local node and Velocity validation",
            new PeerStatus(false, "", "", "peer is not validated")
        ));
        this.beginValidation(configuration.identity());
    }

    @Override
    public ServerIdentity identity() {
        return this.snapshot.get().identity();
    }

    @Override
    public FeatureState featureState() {
        return this.snapshot.get().featureState();
    }

    @Override
    public String statusDetail() {
        return this.snapshot.get().detail();
    }

    @Override
    public PeerStatus peerStatus() {
        return this.snapshot.get().peerStatus();
    }

    @Override
    public Ownership ownership(final WorldPosition position) {
        return this.featureState() == FeatureState.MAINTENANCE
            ? Ownership.MAINTENANCE
            : this.shardManifests.get().ownership(position);
    }

    @Override
    public RemoteOperations remoteOperations() {
        return this.remoteOperations;
    }

    @Override
    public CompletionStage<ReloadResult> reload() {
        if (this.closed.get()) {
            return CompletableFuture.completedFuture(new ReloadResult(false, "Shardingbase is shutting down"));
        }
        final CompletableFuture<ReloadResult> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                final ShardingbaseConfiguration candidate = this.configurationLoader.load();
                final ShardManifestRegistry candidateManifests = ShardManifestRegistry.load(this.serverDirectory);
                this.menuReloader.run();
                this.shardManifests.set(candidateManifests);
                this.beginValidation(candidate.identity());
                result.complete(new ReloadResult(true, "configuration accepted; validation is pending"));
            } catch (final ShardingbaseConfigurationException exception) {
                this.logger.log(Level.SEVERE, "Shardingbase reload rejected; retaining the previous configuration", exception);
                result.complete(new ReloadResult(false, exception.getMessage()));
            } catch (final RuntimeException exception) {
                result.completeExceptionally(exception);
            }
        });
        return result;
    }

    /**
     * Attaches the independently recoverable menu reload operation.
     *
     * @param menuReloader menu reload action
     */
    public void menuReloader(final Runnable menuReloader) {
        this.menuReloader = Objects.requireNonNull(menuReloader, "menuReloader");
    }

    /** Attaches the server-thread executor used for managed player handoff requests. */
    public void playerExecutor(final java.util.concurrent.Executor playerExecutor) {
        this.playerStateCoordinator.serverExecutor(playerExecutor);
        this.remoteOperationCoordinator.start(playerExecutor, this::ownership);
        this.remoteCommandCoordinator.start(playerExecutor);
        this.worldMapCoordinator.start(playerExecutor);
        this.worldTransactionCoordinator.start(playerExecutor);
    }

    /** Renders every generated chunk in a loaded world and publishes a one-use planner link. */
    public CompletionStage<MapPlannerCodec.Link> createWorldPlanner(final org.bukkit.World world) {
        if (this.featureState() != FeatureState.ENABLED) {
            return CompletableFuture.failedFuture(new IllegalStateException(this.statusDetail()));
        }
        return this.worldMapCoordinator.create(world);
    }

    /** Starts an asynchronous lookup for state staged for a joining player. */
    public CompletionStage<Optional<PlayerHandoffCodec.Stage>> fetchPlayerState(final UUID playerId) {
        return this.playerStateCoordinator.fetch(playerId);
    }

    /** Applies a previously fetched state revision on the server thread. */
    public void applyPlayerState(
        final org.bukkit.entity.Player player,
        final Optional<PlayerHandoffCodec.Stage> fetched
    ) throws java.io.IOException {
        this.playerStateCoordinator.applyIfNew(player, fetched);
    }

    /** Captures post-quit state and schedules replication to the currently validated peer. */
    public void replicatePlayerState(final org.bukkit.entity.Player player) {
        final PeerStatus peer = this.peerStatus();
        if (this.playerStateCoordinator.frozen(player.getUniqueId())
            || this.featureState() == FeatureState.ENABLED && peer.available()) {
            this.playerStateCoordinator.captureAndReplicate(player, peer.serverId());
        }
    }

    /** Returns whether source-side interaction is frozen for a managed handoff. */
    public boolean isPlayerStateFrozen(final UUID playerId) {
        return this.playerStateCoordinator.frozen(playerId);
    }

    /** Returns the last successfully loaded network-wide player category selection. */
    public java.util.Set<PlayerDataCategory> playerDataCategories() {
        return this.playerStateCoordinator.categories();
    }

    /** Toggles a logical GUI option at the Velocity SQLite authority. */
    public CompletionStage<java.util.Set<PlayerDataCategory>> togglePlayerDataCategories(
        final java.util.Set<PlayerDataCategory> categories
    ) {
        if (this.featureState() != FeatureState.ENABLED) {
            return CompletableFuture.failedFuture(new IllegalStateException(this.statusDetail()));
        }
        return this.playerStateCoordinator.toggle(categories);
    }

    private void beginValidation(final ServerIdentity identity) {
        final long currentGeneration = this.generation.incrementAndGet();
        final ScheduledFuture<?> previousRetry = this.retryTask;
        if (previousRetry != null) {
            previousRetry.cancel(false);
        }
        this.snapshot.set(new Snapshot(
            identity,
            FeatureState.PENDING,
            "waiting for local node and Velocity validation",
            new PeerStatus(false, "", "", "peer is not validated")
        ));
        this.executor.execute(() -> this.validate(identity, currentGeneration, 0));
    }

    private void validate(final ServerIdentity identity, final long expectedGeneration, final int attempt) {
        if (this.closed.get() || this.generation.get() != expectedGeneration) {
            return;
        }

        final ValidationResult result;
        try {
            result = this.validator.validate(identity);
        } catch (final Exception exception) {
            this.publishDisabled(identity, expectedGeneration, "validation unavailable: " + safeMessage(exception));
            this.scheduleRetry(identity, expectedGeneration, attempt);
            return;
        }

        if (result.accepted()) {
            if (this.generation.get() == expectedGeneration && !this.closed.get()) {
                this.snapshot.set(new Snapshot(
                    identity,
                    FeatureState.ENABLED,
                    result.detail(),
                    new PeerStatus(true, result.peerId(), result.peerName(), result.detail())
                ));
                this.logger.info("Shardingbase features enabled for " + identity.serverName() + " (" + identity.serverId() + ')');
            }
            return;
        }

        this.publishDisabled(identity, expectedGeneration, result.detail());
        this.scheduleRetry(identity, expectedGeneration, attempt);
    }

    private void publishDisabled(final ServerIdentity identity, final long expectedGeneration, final String detail) {
        if (this.generation.get() == expectedGeneration && !this.closed.get()) {
            final boolean maintenance = this.shardManifests.get().isSharded();
            this.snapshot.set(new Snapshot(
                identity,
                maintenance ? FeatureState.MAINTENANCE : FeatureState.DISABLED,
                maintenance ? "shard ownership maintenance lock: " + detail : detail,
                new PeerStatus(false, "", "", detail)
            ));
        }
    }

    private void scheduleRetry(final ServerIdentity identity, final long expectedGeneration, final int attempt) {
        if (this.closed.get() || this.generation.get() != expectedGeneration) {
            return;
        }
        final long delaySeconds = Math.min(MAX_RETRY_DELAY.toSeconds(), 1L << Math.min(attempt, 6));
        this.retryTask = this.executor.schedule(
            () -> this.validate(identity, expectedGeneration, attempt + 1),
            delaySeconds,
            TimeUnit.SECONDS
        );
    }

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        this.generation.incrementAndGet();
        final ScheduledFuture<?> pendingRetry = this.retryTask;
        if (pendingRetry != null) {
            pendingRetry.cancel(true);
        }
        this.executor.shutdownNow();
        this.playerStateCoordinator.close();
        this.remoteOperationCoordinator.close();
        this.remoteCommandCoordinator.close();
        this.worldMapCoordinator.close();
        this.worldTransactionCoordinator.close();
        final Snapshot previous = this.snapshot.get();
        this.snapshot.set(new Snapshot(
            previous.identity(),
            FeatureState.DISABLED,
            "server is shutting down",
            new PeerStatus(false, "", "", "server is shutting down")
        ));
    }

    private static ScheduledExecutorService createExecutor() {
        final ThreadFactory factory = task -> Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Control")
            .unstarted(task);
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, factory);
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }

    private static String safeMessage(final Exception exception) {
        final String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
    }

    private record Snapshot(ServerIdentity identity, FeatureState featureState, String detail, PeerStatus peerStatus) {
        private Snapshot {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(featureState, "featureState");
            Objects.requireNonNull(detail, "detail");
            Objects.requireNonNull(peerStatus, "peerStatus");
        }
    }

    private final class RoutedRemoteOperations implements RemoteOperations {
        @Override
        public CompletionStage<RemoteResult<BlockSnapshot>> readBlock(final WorldPosition position) {
            return this.request(
                RemoteOperationCodec.Operation.READ_BLOCK,
                position,
                "",
                Map.of(),
                response -> new BlockSnapshot(response.value(), response.properties())
            );
        }

        @Override
        public CompletionStage<RemoteResult<Void>> setBlockData(final WorldPosition position, final String blockData) {
            if (blockData == null || blockData.isBlank()) {
                return CompletableFuture.completedFuture(new RemoteResult.ValidationFailure<>("blockData is required"));
            }
            return this.request(
                RemoteOperationCodec.Operation.SET_BLOCK_DATA,
                position,
                blockData,
                Map.of(),
                response -> null
            );
        }

        @Override
        public CompletionStage<RemoteResult<Boolean>> breakBlock(final WorldPosition position) {
            return this.request(
                RemoteOperationCodec.Operation.BREAK_BLOCK,
                position,
                "",
                Map.of(),
                response -> Boolean.parseBoolean(response.value())
            );
        }

        @Override
        public CompletionStage<RemoteResult<UUID>> spawnEntity(final EntitySpawn spawn) {
            if (spawn == null || spawn.entityType() == null || spawn.entityType().isBlank()) {
                return CompletableFuture.completedFuture(new RemoteResult.ValidationFailure<>("entity spawn is required"));
            }
            return this.request(
                RemoteOperationCodec.Operation.SPAWN_ENTITY,
                spawn.position(),
                spawn.entityType(),
                spawn.properties(),
                response -> UUID.fromString(response.value())
            );
        }

        private <T> CompletionStage<RemoteResult<T>> request(
            final RemoteOperationCodec.Operation operation,
            final WorldPosition position,
            final String argument,
            final Map<String, String> properties,
            final java.util.function.Function<RemoteOperationCodec.Response, T> decoder
        ) {
            if (position == null || position.worldKey() == null || position.worldKey().isBlank()) {
                return CompletableFuture.completedFuture(new RemoteResult.ValidationFailure<>("world position is required"));
            }
            if (ShardingbaseRuntime.this.featureState() != FeatureState.ENABLED
                || !ShardingbaseRuntime.this.peerStatus().available()) {
                return CompletableFuture.completedFuture(new RemoteResult.Unavailable<>(
                    "peer is unavailable: " + ShardingbaseRuntime.this.statusDetail()
                ));
            }
            if (ShardingbaseRuntime.this.ownership(position) != Ownership.REMOTE) {
                return CompletableFuture.completedFuture(new RemoteResult.ValidationFailure<>(
                    "position is not owned by the peer shard"
                ));
            }
            final PeerStatus peer = ShardingbaseRuntime.this.peerStatus();
            final RemoteOperationCodec.Request request = new RemoteOperationCodec.Request(
                UUID.randomUUID(),
                ShardingbaseRuntime.this.identity().serverId(),
                operation,
                position.worldKey(),
                position.x(),
                position.y(),
                position.z(),
                argument,
                properties
            );
            return ShardingbaseRuntime.this.remoteOperationCoordinator.request(peer.serverId(), request)
                .handle((response, failure) -> {
                    if (failure != null) {
                        Throwable cause = failure;
                        while (cause.getCause() != null) {
                            cause = cause.getCause();
                        }
                        if (cause instanceof java.util.concurrent.TimeoutException) {
                            return new RemoteResult.Timeout<T>(cause.getMessage());
                        }
                        return new RemoteResult.Unavailable<T>(safeMessage(
                            cause instanceof Exception exception ? exception : new Exception(cause)
                        ));
                    }
                    return switch (response.outcome()) {
                        case SUCCESS -> {
                            try {
                                yield new RemoteResult.Success<T>(decoder.apply(response));
                            } catch (final RuntimeException exception) {
                                yield new RemoteResult.RemoteFailure<T>("invalid peer response: " + safeMessage(exception));
                            }
                        }
                        case VALIDATION_FAILURE -> new RemoteResult.ValidationFailure<T>(response.detail());
                        case REMOTE_FAILURE -> new RemoteResult.RemoteFailure<T>(response.detail());
                    };
                });
        }

    }
}
