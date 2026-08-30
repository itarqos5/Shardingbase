package dev.shardingbase.velocity;

import com.velocitypowered.api.proxy.ProxyServer;
import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.NodeAuthenticationCodec;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import dev.shardingbase.protocol.PlayerSettingsCodec;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ReplayWindow;
import dev.shardingbase.protocol.RemoteCommandCodec;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.protocol.ValidationPayloadCodec;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationRequest;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLServerSocket;
import org.slf4j.Logger;

/** Bounded persistent TLS listener for authenticated node sessions. */
final class ControlServer implements AutoCloseable {
    private static final int CLIENT_TIMEOUT_MILLIS = 15_000;
    private static final int SESSION_QUEUE_CAPACITY = 256;
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(3);

    private final ProxyServer proxy;
    private final Logger logger;
    private final Map<String, String> credentials;
    private final BackendRegistry registry;
    private final PlayerStateStore playerStateStore;
    private final ConcurrentHashMap<String, ClientSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> commandCatalogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, CompletableFuture<RemoteCommandCodec.Response>> pendingCommands =
        new ConcurrentHashMap<>();
    private final SSLServerSocket serverSocket;
    private final ThreadPoolExecutor clients;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread acceptThread;

    ControlServer(
        final ProxyServer proxy,
        final Logger logger,
        final VelocityConfiguration configuration,
        final TlsMaterial tlsMaterial,
        final BackendRegistry registry,
        final PlayerStateStore playerStateStore
    ) throws IOException {
        this.proxy = proxy;
        this.logger = logger;
        this.credentials = configuration.nodeCredentials();
        this.registry = registry;
        this.playerStateStore = playerStateStore;
        this.serverSocket = (SSLServerSocket) tlsMaterial.context().getServerSocketFactory().createServerSocket();
        this.serverSocket.setEnabledProtocols(new String[] {"TLSv1.3"});
        this.serverSocket.bind(new InetSocketAddress(
            InetAddress.getByName(configuration.bindAddress()),
            configuration.controlPort()
        ));
        this.clients = new ThreadPoolExecutor(
            2,
            8,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(64),
            task -> Thread.ofPlatform().daemon(true).name("Shardingbase Velocity Control").unstarted(task),
            new ThreadPoolExecutor.AbortPolicy()
        );
        this.acceptThread = Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Velocity Accept")
            .start(this::acceptLoop);
    }

    private void acceptLoop() {
        while (!this.closed.get()) {
            try {
                final Socket socket = this.serverSocket.accept();
                try {
                    this.clients.execute(() -> this.handle(socket));
                } catch (RuntimeException _) {
                    socket.close();
                    this.logger.warn("Rejected a Shardingbase control connection because the bounded queue is full");
                }
            } catch (final IOException exception) {
                if (!this.closed.get()) {
                    this.logger.error("Shardingbase control accept failed", exception);
                }
            }
        }
    }

    private void handle(final Socket socket) {
        ClientSession session = null;
        try {
            socket.setSoTimeout(CLIENT_TIMEOUT_MILLIS);
            final ProtocolFrame authenticationFrame = FrameCodec.read(socket.getInputStream());
            final String nodeId = authenticationFrame.sourceId();
            final String rejection = this.authenticationRejection(authenticationFrame);
            FrameCodec.write(socket.getOutputStream(), new ProtocolFrame(
                ShardingbaseProtocol.VERSION,
                ProtocolChannel.CONTROL,
                MessageType.AUTHENTICATE_NODE_RESPONSE,
                authenticationFrame.correlationId(),
                "velocity",
                nodeId,
                NodeAuthenticationCodec.encodeResponse(rejection == null, rejection == null ? "authenticated" : rejection)
            ));
            if (rejection != null) {
                return;
            }

            session = new ClientSession(nodeId, socket);
            final ClientSession previous = this.sessions.put(nodeId, session);
            if (previous != null) {
                previous.close();
            }
            session.startWriter();
            this.logger.info("Shardingbase node {} established a persistent control session", nodeId);
            while (!this.closed.get() && !session.closed()) {
                final ProtocolFrame frame = FrameCodec.read(socket.getInputStream());
                this.dispatch(session, frame);
            }
        } catch (final IOException exception) {
            if (!this.closed.get()) {
                this.logger.warn("Shardingbase control session ended: {}", exception.getMessage());
            }
        } finally {
            if (session != null) {
                this.sessions.remove(session.nodeId(), session);
                session.close();
            } else {
                try {
                    socket.close();
                } catch (IOException _) {
                }
            }
        }
    }

    private String authenticationRejection(final ProtocolFrame frame) throws IOException {
        if (frame.version() != ShardingbaseProtocol.VERSION) {
            return "protocol version mismatch";
        }
        if (frame.channel() != ProtocolChannel.CONTROL
            || frame.messageType() != MessageType.AUTHENTICATE_NODE_REQUEST
            || !"velocity".equals(frame.targetId())) {
            return "node authentication must be the first control message";
        }
        final String expected = this.credentials.get(frame.sourceId());
        final String actual = NodeAuthenticationCodec.decodeRequest(frame.payload());
        return expected != null && constantTimeEquals(expected, actual) ? null : "node authentication failed";
    }

    private void dispatch(final ClientSession source, final ProtocolFrame frame) throws IOException {
        if (frame.version() != ShardingbaseProtocol.VERSION || !source.nodeId().equals(frame.sourceId())) {
            source.send(error(frame, "session identity or protocol version mismatch"));
            return;
        }
        if (!source.replayWindow().accept(frame.correlationId())) {
            source.send(error(frame, "replayed correlation ID"));
            return;
        }
        if (!"velocity".equals(frame.targetId())) {
            final String targetNodeId = this.registry.nodeIdForTarget(frame.targetId()).orElse(frame.targetId());
            final ClientSession target = this.sessions.get(targetNodeId);
            if (target == null) {
                source.send(error(frame, "target node is unavailable"));
            } else {
                target.send(frame);
            }
            return;
        }
        switch (frame.messageType()) {
            case HEARTBEAT -> source.send(response(frame, MessageType.HEARTBEAT_ACK, new byte[0]));
            case VALIDATE_BACKEND_REQUEST -> source.send(response(
                frame,
                MessageType.VALIDATE_BACKEND_RESPONSE,
                ValidationPayloadCodec.encodeResponse(this.validate(source.nodeId(), frame))
            ));
            case PLAYER_SNAPSHOT_PREPARE -> source.send(response(
                frame,
                MessageType.PLAYER_SNAPSHOT_ACK,
                this.preparePlayerHandoff(frame)
            ));
            case PLAYER_SNAPSHOT_STAGE -> source.send(response(
                frame,
                MessageType.PLAYER_SNAPSHOT_ACK,
                this.stagePlayerSnapshot(source.nodeId(), frame)
            ));
            case PLAYER_SNAPSHOT_FETCH -> source.send(response(
                frame,
                MessageType.PLAYER_SNAPSHOT_FETCH_RESPONSE,
                this.fetchPlayerSnapshot(source.nodeId(), frame)
            ));
            case PLAYER_SETTINGS_GET -> source.send(response(
                frame,
                MessageType.PLAYER_SETTINGS_RESPONSE,
                PlayerSettingsCodec.encode(this.playerStateStore.categories())
            ));
            case PLAYER_SETTINGS_SET -> {
                final var categories = PlayerSettingsCodec.decode(frame.payload());
                this.playerStateStore.categories(categories);
                source.send(response(
                    frame,
                    MessageType.PLAYER_SETTINGS_RESPONSE,
                    PlayerSettingsCodec.encode(categories)
                ));
            }
            case COMMAND_CATALOG -> {
                final RemoteCommandCodec.Catalog catalog = RemoteCommandCodec.decodeCatalog(frame.payload());
                if (!this.registry.nodeIdForTarget(catalog.backendId()).filter(source.nodeId()::equals).isPresent()) {
                    source.send(error(frame, "command catalog backend is not owned by this node"));
                    break;
                }
                this.commandCatalogs.put(catalog.backendId(), catalog.labels());
                source.send(response(frame, MessageType.COMMAND_CATALOG_ACK, new byte[0]));
            }
            case COMMAND_RESPONSE -> {
                final RemoteCommandCodec.Response commandResponse = RemoteCommandCodec.decodeResponse(frame.payload());
                final CompletableFuture<RemoteCommandCodec.Response> pending =
                    this.pendingCommands.remove(commandResponse.requestId());
                if (pending != null) {
                    pending.complete(commandResponse);
                }
                source.send(response(frame, MessageType.COMMAND_CATALOG_ACK, new byte[0]));
            }
            default -> source.send(error(frame, "unexpected message for Velocity authority"));
        }
    }

    Set<String> commandCatalog(final String backendId) {
        return this.commandCatalogs.getOrDefault(backendId, Set.of());
    }

    CompletableFuture<RemoteCommandCodec.Response> command(
        final BackendRegistry.BackendTarget target,
        final RemoteCommandCodec.Operation operation,
        final String commandLine
    ) {
        final UUID requestId = UUID.randomUUID();
        final CompletableFuture<RemoteCommandCodec.Response> result = new CompletableFuture<>();
        this.pendingCommands.put(requestId, result);
        try {
            final ClientSession session = this.sessions.get(target.nodeId());
            if (session == null) {
                throw new IOException("target node is unavailable");
            }
            session.send(new ProtocolFrame(
                ShardingbaseProtocol.VERSION,
                ProtocolChannel.COMMAND,
                MessageType.COMMAND_REQUEST,
                requestId,
                "velocity",
                target.nodeId(),
                RemoteCommandCodec.encodeRequest(new RemoteCommandCodec.Request(
                    requestId, "velocity", operation, commandLine
                ))
            ));
        } catch (final IOException exception) {
            this.pendingCommands.remove(requestId);
            result.completeExceptionally(exception);
            return result;
        }
        CompletableFuture.delayedExecutor(COMMAND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).execute(() -> {
            final CompletableFuture<RemoteCommandCodec.Response> pending = this.pendingCommands.remove(requestId);
            if (pending != null) {
                pending.completeExceptionally(new TimeoutException("Remote command timed out after three seconds"));
            }
        });
        return result;
    }

    void sendPlayerCapture(final BackendRegistry.BackendTarget source, final PlayerHandoffCodec.Capture capture)
        throws IOException {
        final ClientSession session = this.sessions.get(source.nodeId());
        if (session == null) {
            throw new IOException("source node is unavailable");
        }
        session.send(new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            ProtocolChannel.PLAYER_SYNC,
            MessageType.PLAYER_SNAPSHOT_CAPTURE,
            UUID.randomUUID(),
            "velocity",
            source.nodeId(),
            PlayerHandoffCodec.encodeCapture(capture)
        ));
    }

    private byte[] preparePlayerHandoff(final ProtocolFrame frame) throws IOException {
        final PlayerHandoffCodec.Prepare prepare = PlayerHandoffCodec.decodePrepare(frame.payload());
        if (this.registry.nodeIdForTarget(prepare.targetBackendId()).isEmpty()) {
            return PlayerHandoffCodec.encodeAcknowledgement(new PlayerHandoffCodec.Acknowledgement(
                prepare.playerId(), 1, false, "target backend is not registered"
            ));
        }
        final long revision = this.playerStateStore.reserveRevision(prepare.playerId());
        return PlayerHandoffCodec.encodeAcknowledgement(new PlayerHandoffCodec.Acknowledgement(
            prepare.playerId(), revision, true, "revision reserved"
        ));
    }

    private byte[] stagePlayerSnapshot(final String nodeId, final ProtocolFrame frame) throws IOException {
        final PlayerHandoffCodec.Stage stage = PlayerHandoffCodec.decodeStage(frame.payload());
        final PlayerSnapshot snapshot = stage.snapshot();
        if (!this.registry.nodeIdForTarget(snapshot.sourceBackendId()).filter(nodeId::equals).isPresent()) {
            return PlayerHandoffCodec.encodeAcknowledgement(new PlayerHandoffCodec.Acknowledgement(
                snapshot.playerId(), snapshot.revision(), false, "snapshot source is not owned by this node"
            ));
        }
        if (this.registry.nodeIdForTarget(stage.targetBackendId()).isEmpty()) {
            return PlayerHandoffCodec.encodeAcknowledgement(new PlayerHandoffCodec.Acknowledgement(
                snapshot.playerId(), snapshot.revision(), false, "target backend is not registered"
            ));
        }
        final PlayerStateStore.StageResult result = this.playerStateStore.acceptRevision(
            snapshot.playerId(),
            snapshot.revision(),
            snapshot.sourceBackendId(),
            PlayerHandoffCodec.encodeStage(stage)
        );
        final boolean accepted = result == PlayerStateStore.StageResult.STORED
            || result == PlayerStateStore.StageResult.DUPLICATE;
        return PlayerHandoffCodec.encodeAcknowledgement(new PlayerHandoffCodec.Acknowledgement(
            snapshot.playerId(), snapshot.revision(), accepted, result.name().toLowerCase(java.util.Locale.ROOT)
        ));
    }

    private byte[] fetchPlayerSnapshot(final String nodeId, final ProtocolFrame frame) throws IOException {
        final PlayerHandoffCodec.Fetch fetch = PlayerHandoffCodec.decodeFetch(frame.payload());
        if (!this.registry.nodeIdForTarget(fetch.targetBackendId()).filter(nodeId::equals).isPresent()) {
            throw new IOException("snapshot target is not owned by this node");
        }
        final var stored = this.playerStateStore.load(fetch.playerId());
        PlayerHandoffCodec.Stage stage = null;
        if (stored.isPresent()) {
            final PlayerHandoffCodec.Stage candidate = PlayerHandoffCodec.decodeStage(stored.orElseThrow().snapshot());
            if (candidate.targetBackendId().equals(fetch.targetBackendId())) {
                stage = candidate;
            }
        }
        return PlayerHandoffCodec.encodeFetchResponse(new PlayerHandoffCodec.FetchResponse(stage));
    }

    private ValidationResponse validate(final String nodeId, final ProtocolFrame frame) throws IOException {
        final ValidationRequest request = ValidationPayloadCodec.decodeRequest(frame.payload());
        final String expectedCredential = this.credentials.get(nodeId);
        if (expectedCredential == null || !constantTimeEquals(expectedCredential, request.credential())) {
            return rejected("node authentication failed");
        }
        if (this.proxy.getServer(request.serverName()).isEmpty()) {
            return rejected("server-name is not present in Velocity's servers configuration");
        }
        return this.registry.register(nodeId, request);
    }

    private static ProtocolFrame response(
        final ProtocolFrame request,
        final MessageType messageType,
        final byte[] payload
    ) {
        return new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            request.channel(),
            messageType,
            request.correlationId(),
            "velocity",
            request.sourceId(),
            payload
        );
    }

    private static ProtocolFrame error(final ProtocolFrame request, final String detail) {
        return response(request, MessageType.ERROR, detail.getBytes(StandardCharsets.UTF_8));
    }

    private static ValidationResponse rejected(final String detail) {
        return new ValidationResponse(false, detail, "", "");
    }

    private static boolean constantTimeEquals(final String expected, final String actual) {
        return MessageDigest.isEqual(
            expected.getBytes(StandardCharsets.UTF_8),
            actual.getBytes(StandardCharsets.UTF_8)
        );
    }

    @Override
    public void close() throws IOException {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        this.serverSocket.close();
        this.sessions.values().forEach(ClientSession::close);
        this.sessions.clear();
        this.clients.shutdownNow();
        this.pendingCommands.forEach((id, future) -> future.completeExceptionally(
            new IOException("Velocity controller is shutting down")
        ));
        this.pendingCommands.clear();
        this.acceptThread.interrupt();
    }

    private static final class ClientSession {
        private final String nodeId;
        private final Socket socket;
        private final ReplayWindow replayWindow = new ReplayWindow(16_384, Duration.ofMinutes(15));
        private final ArrayBlockingQueue<ProtocolFrame> outbound = new ArrayBlockingQueue<>(SESSION_QUEUE_CAPACITY);
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile Thread writer;

        private ClientSession(final String nodeId, final Socket socket) {
            this.nodeId = nodeId;
            this.socket = socket;
        }

        private void startWriter() {
            this.writer = Thread.ofPlatform()
                .daemon(true)
                .name("Shardingbase Velocity Writer " + this.nodeId)
                .start(this::writeLoop);
        }

        private void send(final ProtocolFrame frame) throws IOException {
            if (this.closed.get() || !this.outbound.offer(frame)) {
                this.close();
                throw new IOException("Node session outbound queue is unavailable");
            }
        }

        private void writeLoop() {
            try {
                while (!this.closed.get()) {
                    final ProtocolFrame frame = this.outbound.poll(1, TimeUnit.SECONDS);
                    if (frame != null) {
                        FrameCodec.write(this.socket.getOutputStream(), frame);
                    }
                }
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            } catch (IOException _) {
                this.close();
            }
        }

        private String nodeId() {
            return this.nodeId;
        }

        private ReplayWindow replayWindow() {
            return this.replayWindow;
        }

        private boolean closed() {
            return this.closed.get();
        }

        private void close() {
            if (!this.closed.compareAndSet(false, true)) {
                return;
            }
            final Thread currentWriter = this.writer;
            if (currentWriter != null) {
                currentWriter.interrupt();
            }
            try {
                this.socket.close();
            } catch (IOException _) {
            }
        }
    }
}
