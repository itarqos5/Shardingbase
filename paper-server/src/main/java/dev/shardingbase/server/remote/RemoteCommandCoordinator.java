package dev.shardingbase.server.remote;

import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.RemoteCommandCodec;
import dev.shardingbase.server.validation.LocalNodeClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.bukkit.Bukkit;
import org.bukkit.craftbukkit.command.CraftConsoleCommandSender;

/** Publishes the local command tree and executes allowlisted proxy-routed commands as captured console calls. */
public final class RemoteCommandCoordinator implements AutoCloseable {
    private static final int COMPLETED_LIMIT = 4_096;
    private static final int OUTPUT_LINE_LIMIT = 256;
    private static final int OUTPUT_CHARACTER_LIMIT = 4_096;

    private final String backendId;
    private final Logger logger;
    private final LocalNodeClient node = new LocalNodeClient();
    private final ScheduledExecutorService io = Executors.newScheduledThreadPool(2, task -> Thread.ofPlatform()
        .daemon(true)
        .name("Shardingbase Remote Command")
        .unstarted(task));
    private final Map<UUID, RemoteCommandCodec.Response> completed = new LinkedHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Executor serverExecutor;
    private volatile Thread pollThread;

    public RemoteCommandCoordinator(final String backendId, final Logger logger) {
        this.backendId = backendId;
        this.logger = logger;
    }

    public void start(final Executor serverExecutor) {
        this.serverExecutor = java.util.Objects.requireNonNull(serverExecutor, "serverExecutor");
        if (this.pollThread == null) {
            this.pollThread = Thread.ofPlatform()
                .daemon(true)
                .name("Shardingbase Remote Command Poll")
                .start(this::pollLoop);
            this.io.scheduleWithFixedDelay(this::requestCatalogPublish, 0, 30, TimeUnit.SECONDS);
        }
    }

    private void requestCatalogPublish() {
        final Executor executor = this.serverExecutor;
        if (executor != null && !this.closed.get()) {
            executor.execute(this::publishCatalog);
        }
    }

    private void publishCatalog() {
        final Set<String> labels = new LinkedHashSet<>();
        for (final String label : Bukkit.getCommandMap().getKnownCommands().keySet()) {
            final String normalized = label.toLowerCase(Locale.ROOT);
            if (normalized.matches("[a-z0-9_.:-]+")) {
                labels.add(normalized);
            }
        }
        this.io.execute(() -> {
            try {
                final ProtocolFrame response = this.node.request(
                    this.backendId,
                    ProtocolChannel.COMMAND,
                    MessageType.COMMAND_CATALOG,
                    "velocity",
                    RemoteCommandCodec.encodeCatalog(new RemoteCommandCodec.Catalog(this.backendId, labels))
                );
                if (response.messageType() != MessageType.COMMAND_CATALOG_ACK) {
                    throw new IOException("Velocity rejected the command catalog");
                }
            } catch (final IOException exception) {
                if (!this.closed.get()) {
                    this.logger.log(Level.FINE, "Unable to publish the Shardingbase command catalog", exception);
                }
            }
        });
    }

    private void pollLoop() {
        while (!this.closed.get()) {
            try {
                final ProtocolFrame frame = this.node.request(
                    this.backendId,
                    ProtocolChannel.COMMAND,
                    MessageType.BACKEND_POLL,
                    "node-local",
                    new byte[0]
                );
                if (frame.messageType() == MessageType.COMMAND_REQUEST) {
                    this.receive(RemoteCommandCodec.decodeRequest(frame.payload()));
                }
            } catch (final IOException exception) {
                if (!this.closed.get() && !pause()) {
                    return;
                }
            }
        }
    }

    private void receive(final RemoteCommandCodec.Request request) {
        final RemoteCommandCodec.Response duplicate;
        synchronized (this.completed) {
            duplicate = this.completed.get(request.requestId());
        }
        if (duplicate != null) {
            this.respond(duplicate);
            return;
        }
        final Executor executor = this.serverExecutor;
        if (executor == null) {
            this.respond(new RemoteCommandCodec.Response(
                request.requestId(), RemoteCommandCodec.Outcome.FAILURE, "server executor is unavailable", List.of()
            ));
            return;
        }
        executor.execute(() -> {
            final RemoteCommandCodec.Response response = this.execute(request);
            synchronized (this.completed) {
                this.completed.put(request.requestId(), response);
                while (this.completed.size() > COMPLETED_LIMIT) {
                    this.completed.remove(this.completed.keySet().iterator().next());
                }
            }
            this.respond(response);
        });
    }

    private RemoteCommandCodec.Response execute(final RemoteCommandCodec.Request request) {
        try {
            final CapturedConsoleSender sender = new CapturedConsoleSender();
            if (request.operation() == RemoteCommandCodec.Operation.SUGGEST) {
                final List<String> suggestions = Bukkit.getCommandMap().tabComplete(sender, request.commandLine());
                return new RemoteCommandCodec.Response(
                    request.requestId(), RemoteCommandCodec.Outcome.SUCCESS, "suggestions", bounded(suggestions)
                );
            }
            final boolean dispatched = Bukkit.dispatchCommand(sender, request.commandLine());
            final List<String> output = sender.lines();
            if (!dispatched || sender.playerOnlyRejection()) {
                return new RemoteCommandCodec.Response(
                    request.requestId(), RemoteCommandCodec.Outcome.REJECTED,
                    sender.playerOnlyRejection()
                        ? "command requires a real player on the remote server"
                        : "command was rejected by the remote server",
                    output
                );
            }
            return new RemoteCommandCodec.Response(
                request.requestId(), RemoteCommandCodec.Outcome.SUCCESS, "command executed", output
            );
        } catch (final RuntimeException exception) {
            this.logger.log(Level.WARNING, "Remote command failed on the server thread", exception);
            final String detail = exception.getMessage() == null
                ? exception.getClass().getSimpleName()
                : exception.getMessage();
            return new RemoteCommandCodec.Response(
                request.requestId(), RemoteCommandCodec.Outcome.FAILURE, detail, List.of()
            );
        }
    }

    private void respond(final RemoteCommandCodec.Response response) {
        this.io.execute(() -> {
            try {
                this.node.request(
                    this.backendId,
                    ProtocolChannel.COMMAND,
                    MessageType.COMMAND_RESPONSE,
                    "velocity",
                    RemoteCommandCodec.encodeResponse(response)
                );
            } catch (final IOException exception) {
                if (!this.closed.get()) {
                    this.logger.log(Level.WARNING, "Unable to return remote command output", exception);
                }
            }
        });
    }

    private static List<String> bounded(final List<String> values) {
        final List<String> result = new ArrayList<>(Math.min(values.size(), OUTPUT_LINE_LIMIT));
        for (final String value : values) {
            if (result.size() == OUTPUT_LINE_LIMIT) {
                break;
            }
            result.add(value.length() > OUTPUT_CHARACTER_LIMIT ? value.substring(0, OUTPUT_CHARACTER_LIMIT) : value);
        }
        return List.copyOf(result);
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

    private static final class CapturedConsoleSender extends CraftConsoleCommandSender {
        private final List<String> lines = new ArrayList<>();

        @Override
        public void sendRawMessage(final String message) {
            this.add(message);
        }

        @Override
        public void sendRawMessage(final UUID sender, final String message) {
            this.add(message);
        }

        @Override
        public void sendMessage(final Component message) {
            this.add(PlainTextComponentSerializer.plainText().serialize(message));
        }

        private void add(final String message) {
            if (this.lines.size() >= OUTPUT_LINE_LIMIT) {
                return;
            }
            for (final String line : message.split("\\R", -1)) {
                if (this.lines.size() >= OUTPUT_LINE_LIMIT) {
                    return;
                }
                this.lines.add(line.length() > OUTPUT_CHARACTER_LIMIT ? line.substring(0, OUTPUT_CHARACTER_LIMIT) : line);
            }
        }

        private List<String> lines() {
            return List.copyOf(this.lines);
        }

        private boolean playerOnlyRejection() {
            return this.lines.stream().map(line -> line.toLowerCase(Locale.ROOT)).anyMatch(line ->
                line.contains("must be a player")
                    || line.contains("only players")
                    || line.contains("player-only")
                    || line.contains("not a player")
            );
        }
    }
}
