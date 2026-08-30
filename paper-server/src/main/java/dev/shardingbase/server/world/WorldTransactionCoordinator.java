package dev.shardingbase.server.world;

import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.WorldTransactionCodec;
import dev.shardingbase.protocol.WorldTransactionCodec.Operation;
import dev.shardingbase.protocol.WorldTransactionCodec.Outcome;
import dev.shardingbase.protocol.WorldTransactionCodec.Request;
import dev.shardingbase.protocol.WorldTransactionCodec.Response;
import dev.shardingbase.server.validation.LocalNodeClient;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.minecraft.server.MinecraftServer;

/** Saves and flushes the local server before authorizing its node's offline work. */
public final class WorldTransactionCoordinator implements AutoCloseable {
    private final String backendId;
    private final Logger logger;
    private final LocalNodeClient node = new LocalNodeClient();
    private final ExecutorService io = Executors.newSingleThreadExecutor(task -> Thread.ofPlatform()
        .daemon(true)
        .name("Shardingbase World Transaction Response")
        .unstarted(task));
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Executor serverExecutor;
    private volatile Thread pollThread;

    public WorldTransactionCoordinator(final String backendId, final Logger logger) {
        this.backendId = backendId;
        this.logger = logger;
    }

    public void start(final Executor serverExecutor) {
        this.serverExecutor = java.util.Objects.requireNonNull(serverExecutor, "serverExecutor");
        if (this.pollThread == null) {
            this.pollThread = Thread.ofPlatform()
                .daemon(true)
                .name("Shardingbase World Transaction Poll")
                .start(this::pollLoop);
        }
    }

    private void pollLoop() {
        while (!this.closed.get()) {
            try {
                final ProtocolFrame frame = this.node.request(
                    this.backendId,
                    ProtocolChannel.WORLD_TRANSACTION,
                    MessageType.BACKEND_POLL,
                    "node-local",
                    new byte[0]
                );
                if (frame.messageType() == MessageType.WORLD_TRANSACTION_REQUEST) {
                    this.receive(WorldTransactionCodec.decodeRequest(frame.payload()));
                }
            } catch (final IOException exception) {
                if (!this.closed.get() && !pause()) {
                    return;
                }
            }
        }
    }

    private void receive(final Request request) {
        if (request.operation() != Operation.AUTHORIZE_AND_SAVE) {
            this.respond(request, Outcome.REJECTED, "backend only authorizes the save-and-flush phase");
            return;
        }
        final var manifest = request.signedManifest().manifest();
        if (!this.backendId.equals(manifest.sourceBackendId())
            && !this.backendId.equals(manifest.targetBackendId())) {
            this.respond(request, Outcome.REJECTED, "transaction manifest does not include this backend");
            return;
        }
        final Executor executor = this.serverExecutor;
        if (executor == null) {
            this.respond(request, Outcome.FAILED, "server-thread executor is unavailable");
            return;
        }
        executor.execute(() -> {
            try {
                final boolean saved = MinecraftServer.getServer().saveEverything(false, true, true);
                this.respond(
                    request,
                    saved ? Outcome.READY : Outcome.FAILED,
                    saved
                        ? "players, worlds, and storage were saved and flushed"
                        : "server reported that no world data was saved"
                );
            } catch (final RuntimeException exception) {
                this.logger.log(Level.SEVERE, "Unable to save and flush before a Shardingbase transaction", exception);
                this.respond(request, Outcome.FAILED, safeMessage(exception));
            }
        });
    }

    private void respond(final Request request, final Outcome outcome, final String detail) {
        this.io.execute(() -> {
            try {
                final var manifest = request.signedManifest().manifest();
                final Response response = new Response(
                    manifest.transactionId(),
                    request.operation(),
                    outcome,
                    detail,
                    true,
                    ProcessHandle.current().pid(),
                    -1,
                    -1L,
                    WorldTransactionCodec.digest(manifest)
                );
                final ProtocolFrame acknowledgement = this.node.request(
                    this.backendId,
                    ProtocolChannel.WORLD_TRANSACTION,
                    MessageType.WORLD_TRANSACTION_RESPONSE,
                    "node-local",
                    WorldTransactionCodec.encodeResponse(response)
                );
                if (acknowledgement.messageType() != MessageType.BACKEND_SEND_ACK) {
                    throw new IOException("node rejected backend transaction authorization");
                }
            } catch (final IOException exception) {
                if (!this.closed.get()) {
                    this.logger.log(Level.WARNING, "Unable to acknowledge a Shardingbase world transaction", exception);
                }
            }
        });
    }

    private static boolean pause() {
        try {
            TimeUnit.SECONDS.sleep(1);
            return true;
        } catch (final InterruptedException _) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String safeMessage(final RuntimeException exception) {
        final String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
    }

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        final Thread current = this.pollThread;
        if (current != null) {
            current.interrupt();
        }
        this.io.shutdownNow();
    }
}
