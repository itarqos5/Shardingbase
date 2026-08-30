package dev.shardingbase.server.remote;

import dev.shardingbase.api.Ownership;
import dev.shardingbase.api.WorldPosition;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.RemoteOperationCodec;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.server.validation.LocalNodeClient;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.kyori.adventure.text.Component;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.NamespacedKey;
import org.bukkit.Registry;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.entity.Entity;
import org.bukkit.entity.EntityType;

/** Correlates peer API requests and executes inbound operations on the server thread. */
public final class RemoteOperationCoordinator implements AutoCloseable {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static final int COMPLETED_LIMIT = 4_096;

    private final String backendId;
    private final Logger logger;
    private final LocalNodeClient node = new LocalNodeClient();
    private final ConcurrentHashMap<UUID, CompletableFuture<RemoteOperationCodec.Response>> pending =
        new ConcurrentHashMap<>();
    private final Map<UUID, RemoteOperationCodec.Response> completed = new LinkedHashMap<>();
    private final ScheduledExecutorService io = Executors.newScheduledThreadPool(2, task -> Thread.ofPlatform()
        .daemon(true)
        .name("Shardingbase Remote Operation")
        .unstarted(task));
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Executor serverExecutor;
    private volatile Function<WorldPosition, Ownership> ownership = position -> Ownership.MAINTENANCE;
    private volatile Thread pollThread;

    public RemoteOperationCoordinator(final String backendId, final Logger logger) {
        this.backendId = backendId;
        this.logger = logger;
    }

    public void start(final Executor serverExecutor, final Function<WorldPosition, Ownership> ownership) {
        this.serverExecutor = java.util.Objects.requireNonNull(serverExecutor, "serverExecutor");
        this.ownership = java.util.Objects.requireNonNull(ownership, "ownership");
        if (this.pollThread == null) {
            this.pollThread = Thread.ofPlatform()
                .daemon(true)
                .name("Shardingbase Remote Operation Poll")
                .start(this::pollLoop);
        }
    }

    public CompletableFuture<RemoteOperationCodec.Response> request(
        final String targetBackendId,
        final RemoteOperationCodec.Request request
    ) {
        if (this.closed.get()) {
            return CompletableFuture.failedFuture(new IOException("Remote operation coordinator is closed"));
        }
        final CompletableFuture<RemoteOperationCodec.Response> future = new CompletableFuture<>();
        this.pending.put(request.operationId(), future);
        this.io.execute(() -> {
            try {
                final ProtocolFrame acknowledgement = this.node.request(
                    this.backendId,
                    ProtocolChannel.REMOTE_OPERATION,
                    MessageType.REMOTE_OPERATION_REQUEST,
                    targetBackendId,
                    RemoteOperationCodec.encodeRequest(request)
                );
                if (acknowledgement.messageType() != MessageType.BACKEND_SEND_ACK) {
                    throw new IOException("Node rejected remote operation relay");
                }
            } catch (final IOException exception) {
                final CompletableFuture<RemoteOperationCodec.Response> removed = this.pending.remove(request.operationId());
                if (removed != null) {
                    removed.completeExceptionally(exception);
                }
            }
        });
        this.io.schedule(() -> {
            final CompletableFuture<RemoteOperationCodec.Response> removed = this.pending.remove(request.operationId());
            if (removed != null) {
                removed.completeExceptionally(new TimeoutException("Remote operation timed out after three seconds"));
            }
        }, TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return future;
    }

    private void pollLoop() {
        while (!this.closed.get()) {
            try {
                final ProtocolFrame frame = this.node.request(
                    this.backendId,
                    ProtocolChannel.REMOTE_OPERATION,
                    MessageType.BACKEND_POLL,
                    "node-local",
                    new byte[0]
                );
                if (frame.messageType() == MessageType.REMOTE_OPERATION_RESPONSE) {
                    final RemoteOperationCodec.Response response = RemoteOperationCodec.decodeResponse(frame.payload());
                    final CompletableFuture<RemoteOperationCodec.Response> future = this.pending.remove(response.operationId());
                    if (future != null) {
                        future.complete(response);
                    }
                } else if (frame.messageType() == MessageType.REMOTE_OPERATION_REQUEST) {
                    this.receive(RemoteOperationCodec.decodeRequest(frame.payload()));
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

    private void receive(final RemoteOperationCodec.Request request) {
        final RemoteOperationCodec.Response duplicate;
        synchronized (this.completed) {
            duplicate = this.completed.get(request.operationId());
        }
        if (duplicate != null) {
            this.respond(request.originBackendId(), duplicate);
            return;
        }
        final Executor executor = this.serverExecutor;
        if (executor == null) {
            this.respond(request.originBackendId(), failure(
                request.operationId(), RemoteOperationCodec.Outcome.REMOTE_FAILURE, "server executor is unavailable"
            ));
            return;
        }
        executor.execute(() -> {
            final RemoteOperationCodec.Response response = this.execute(request);
            synchronized (this.completed) {
                this.completed.put(request.operationId(), response);
                while (this.completed.size() > COMPLETED_LIMIT) {
                    this.completed.remove(this.completed.keySet().iterator().next());
                }
            }
            this.respond(request.originBackendId(), response);
        });
    }

    private RemoteOperationCodec.Response execute(final RemoteOperationCodec.Request request) {
        final WorldPosition position = new WorldPosition(request.worldKey(), request.x(), request.y(), request.z());
        if (this.ownership.apply(position) != Ownership.LOCAL) {
            return failure(request.operationId(), RemoteOperationCodec.Outcome.VALIDATION_FAILURE,
                "target position is not owned by this shard");
        }
        final NamespacedKey worldKey = NamespacedKey.fromString(request.worldKey());
        final World world = worldKey == null ? null : Bukkit.getWorld(worldKey);
        if (world == null || request.y() < world.getMinHeight() || request.y() >= world.getMaxHeight()) {
            return failure(request.operationId(), RemoteOperationCodec.Outcome.VALIDATION_FAILURE,
                "target world or height is invalid");
        }
        try {
            final Block block = world.getBlockAt(request.x(), request.y(), request.z());
            return switch (request.operation()) {
                case READ_BLOCK -> new RemoteOperationCodec.Response(
                    request.operationId(),
                    RemoteOperationCodec.Outcome.SUCCESS,
                    "block snapshot read",
                    block.getBlockData().getAsString(),
                    Map.of()
                );
                case SET_BLOCK_DATA -> {
                    block.setBlockData(Bukkit.createBlockData(request.argument()), true);
                    yield success(request.operationId(), "block data set", "");
                }
                case BREAK_BLOCK -> success(
                    request.operationId(),
                    "block break executed",
                    Boolean.toString(block.breakNaturally())
                );
                case SPAWN_ENTITY -> this.spawn(request, world);
            };
        } catch (final IllegalArgumentException exception) {
            return failure(request.operationId(), RemoteOperationCodec.Outcome.VALIDATION_FAILURE, exception.getMessage());
        } catch (final RuntimeException exception) {
            this.logger.log(Level.WARNING, "Remote operation failed on the server thread", exception);
            return failure(request.operationId(), RemoteOperationCodec.Outcome.REMOTE_FAILURE,
                exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage());
        }
    }

    private RemoteOperationCodec.Response spawn(final RemoteOperationCodec.Request request, final World world) {
        final NamespacedKey typeKey = NamespacedKey.fromString(request.argument());
        final EntityType type = typeKey == null ? null : Registry.ENTITY_TYPE.get(typeKey);
        if (type == null || !type.isSpawnable()) {
            return failure(request.operationId(), RemoteOperationCodec.Outcome.VALIDATION_FAILURE,
                "entity type is not spawnable");
        }
        for (final String key : request.properties().keySet()) {
            if (!"custom-name".equals(key)) {
                return failure(request.operationId(), RemoteOperationCodec.Outcome.VALIDATION_FAILURE,
                    "unsupported entity property: " + key);
            }
        }
        final Entity entity = world.spawnEntity(
            new Location(world, request.x() + 0.5D, request.y(), request.z() + 0.5D),
            type
        );
        final String customName = request.properties().get("custom-name");
        if (customName != null) {
            entity.customName(Component.text(customName));
        }
        return success(request.operationId(), "entity spawned", entity.getUniqueId().toString());
    }

    private void respond(final String targetBackendId, final RemoteOperationCodec.Response response) {
        this.io.execute(() -> {
            try {
                this.node.request(
                    this.backendId,
                    ProtocolChannel.REMOTE_OPERATION,
                    MessageType.REMOTE_OPERATION_RESPONSE,
                    targetBackendId,
                    RemoteOperationCodec.encodeResponse(response)
                );
            } catch (final IOException exception) {
                this.logger.log(Level.WARNING, "Unable to relay a remote operation response", exception);
            }
        });
    }

    private static RemoteOperationCodec.Response success(final UUID id, final String detail, final String value) {
        return new RemoteOperationCodec.Response(id, RemoteOperationCodec.Outcome.SUCCESS, detail, value, Map.of());
    }

    private static RemoteOperationCodec.Response failure(
        final UUID id,
        final RemoteOperationCodec.Outcome outcome,
        final String detail
    ) {
        return new RemoteOperationCodec.Response(id, outcome, detail == null ? "operation failed" : detail, "", Map.of());
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
        this.pending.forEach((id, future) -> future.completeExceptionally(new IOException("Server is shutting down")));
        this.pending.clear();
    }
}
