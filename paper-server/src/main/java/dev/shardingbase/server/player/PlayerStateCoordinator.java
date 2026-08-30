package dev.shardingbase.server.player;

import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bukkit.entity.Player;

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
        this.revisions.markApplied(snapshot.playerId(), snapshot.revision());
    }

    /**
     * Captures final post-quit state on the server thread and asynchronously replicates it to the peer.
     */
    public void captureAndReplicate(final Player player, final String peerBackendId) {
        if (peerBackendId == null || peerBackendId.isBlank()) {
            return;
        }
        final PlayerSnapshot captured;
        try {
            captured = this.adapter.capture(player, 1, this.backendId, ALL_CATEGORIES);
        } catch (final IOException | RuntimeException exception) {
            this.logger.log(Level.WARNING, "Unable to capture portable player state for " + player.getUniqueId(), exception);
            return;
        }
        try {
            this.transport.execute(() -> {
                try {
                    final long revision = this.handoff.prepare(captured.playerId(), peerBackendId, ALL_CATEGORIES);
                    this.handoff.stage(peerBackendId, new PlayerSnapshot(
                        captured.playerId(),
                        revision,
                        this.backendId,
                        captured.categories()
                    ));
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

    @Override
    public void close() {
        this.transport.shutdownNow();
    }
}
