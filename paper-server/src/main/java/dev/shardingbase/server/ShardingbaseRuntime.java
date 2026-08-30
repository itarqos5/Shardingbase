package dev.shardingbase.server;

import dev.shardingbase.api.FeatureState;
import dev.shardingbase.api.BlockSnapshot;
import dev.shardingbase.api.EntitySpawn;
import dev.shardingbase.api.Ownership;
import dev.shardingbase.api.PeerStatus;
import dev.shardingbase.api.RemoteOperations;
import dev.shardingbase.api.RemoteResult;
import dev.shardingbase.api.ServerIdentity;
import dev.shardingbase.api.ShardingbaseService;
import dev.shardingbase.api.WorldPosition;
import dev.shardingbase.server.config.ShardingbaseConfiguration;
import dev.shardingbase.server.config.ShardingbaseConfigurationException;
import dev.shardingbase.server.config.ShardingbaseConfigurationLoader;
import dev.shardingbase.server.validation.BackendValidator;
import dev.shardingbase.server.validation.LocalNodeValidator;
import dev.shardingbase.server.validation.ValidationResult;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.UUID;
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
    private final BackendValidator validator;
    private final Logger logger;
    private final ScheduledExecutorService executor;
    private final AtomicReference<Snapshot> snapshot;
    private final AtomicLong generation = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile @Nullable ScheduledFuture<?> retryTask;
    private volatile Runnable menuReloader = () -> {
    };
    private final RemoteOperations remoteOperations = new DisabledRemoteOperations();

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
            createExecutor()
        );
    }

    ShardingbaseRuntime(
        final ShardingbaseConfigurationLoader configurationLoader,
        final BackendValidator validator,
        final Logger logger,
        final ScheduledExecutorService executor
    ) throws ShardingbaseConfigurationException {
        this.configurationLoader = configurationLoader;
        this.validator = validator;
        this.logger = logger;
        this.executor = executor;
        final ShardingbaseConfiguration configuration = this.configurationLoader.load();
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
        return this.featureState() == FeatureState.MAINTENANCE ? Ownership.MAINTENANCE : Ownership.UNSHARDED;
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
                this.menuReloader.run();
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
            this.snapshot.set(new Snapshot(
                identity,
                FeatureState.DISABLED,
                detail,
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

    private final class DisabledRemoteOperations implements RemoteOperations {
        @Override
        public CompletionStage<RemoteResult<BlockSnapshot>> readBlock(final WorldPosition position) {
            return unavailable();
        }

        @Override
        public CompletionStage<RemoteResult<Void>> setBlockData(final WorldPosition position, final String blockData) {
            return unavailable();
        }

        @Override
        public CompletionStage<RemoteResult<Boolean>> breakBlock(final WorldPosition position) {
            return unavailable();
        }

        @Override
        public CompletionStage<RemoteResult<UUID>> spawnEntity(final EntitySpawn spawn) {
            return unavailable();
        }

        private <T> CompletionStage<RemoteResult<T>> unavailable() {
            return CompletableFuture.completedFuture(new RemoteResult.Unavailable<>(ShardingbaseRuntime.this.statusDetail()));
        }
    }
}
