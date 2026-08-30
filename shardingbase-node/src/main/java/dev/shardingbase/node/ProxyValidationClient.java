package dev.shardingbase.node;

import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.NodeAuthenticationCodec;
import dev.shardingbase.protocol.NodeAuthenticationCodec.AuthenticationResponse;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.protocol.ValidationPayloadCodec;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationRequest;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/** Persistent pinned-TLS session from one node to the Velocity authority. */
final class ProxyValidationClient implements AutoCloseable {
    static final String CONTROLLER_URI_ENVIRONMENT_VARIABLE = "SHARDINGBASE_CONTROLLER_URI";
    static final String CERTIFICATE_FINGERPRINT_ENVIRONMENT_VARIABLE = "SHARDINGBASE_CERTIFICATE_SHA256";
    static final String CREDENTIAL_ENVIRONMENT_VARIABLE = "SHARDINGBASE_NODE_CREDENTIAL";
    static final String NODE_ID_ENVIRONMENT_VARIABLE = "SHARDINGBASE_NODE_ID";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final int OUTBOUND_QUEUE_CAPACITY = 256;

    private final Map<String, String> environment;
    private final ArrayBlockingQueue<ProtocolFrame> outbound = new ArrayBlockingQueue<>(OUTBOUND_QUEUE_CAPACITY);
    private final ConcurrentHashMap<UUID, CompletableFuture<ProtocolFrame>> pending = new ConcurrentHashMap<>();
    private final AtomicReference<ActiveConnection> connection = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService connectionExecutor;
    private final ScheduledExecutorService heartbeatExecutor;
    private volatile Consumer<ProtocolFrame> pushHandler = frame -> {
    };

    ProxyValidationClient() {
        this(System.getenv());
    }

    ProxyValidationClient(final Map<String, String> environment) {
        this.environment = Map.copyOf(environment);
        this.connectionExecutor = Executors.newSingleThreadExecutor(task -> Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Proxy Session")
            .unstarted(task));
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Proxy Heartbeat")
            .unstarted(task));
        if (this.configurationAvailable()) {
            this.connectionExecutor.execute(this::connectLoop);
            this.heartbeatExecutor.scheduleAtFixedRate(this::heartbeat, 5, 5, TimeUnit.SECONDS);
        }
    }

    ValidationResponse validate(
        final String serverId,
        final String serverName,
        final String minecraftVersion,
        final String shardingbaseVersion
    ) throws IOException {
        if (!this.configurationAvailable()) {
            return new ValidationResponse(
                false,
                "node controller URI, credential, node ID, and certificate fingerprint are required",
                "",
                ""
            );
        }
        final ValidationRequest request = new ValidationRequest(
            this.environment.get(CREDENTIAL_ENVIRONMENT_VARIABLE),
            serverId,
            serverName,
            minecraftVersion,
            shardingbaseVersion
        );
        final ProtocolFrame response = this.request(
            ProtocolChannel.CONTROL,
            MessageType.VALIDATE_BACKEND_REQUEST,
            "velocity",
            ValidationPayloadCodec.encodeRequest(request)
        );
        if (response.messageType() != MessageType.VALIDATE_BACKEND_RESPONSE) {
            throw new IOException("Velocity returned an unexpected validation response");
        }
        return ValidationPayloadCodec.decodeResponse(response.payload());
    }

    ProtocolFrame request(
        final ProtocolChannel channel,
        final MessageType messageType,
        final String targetId,
        final byte[] payload
    ) throws IOException {
        if (this.closed.get()) {
            throw new IOException("Shardingbase proxy session is closed");
        }
        if (this.connection.get() == null) {
            throw new IOException("Shardingbase proxy session is not connected");
        }
        final UUID correlationId = UUID.randomUUID();
        final ProtocolFrame request = new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            channel,
            messageType,
            correlationId,
            this.nodeId(),
            targetId,
            payload
        );
        final CompletableFuture<ProtocolFrame> response = new CompletableFuture<>();
        this.pending.put(correlationId, response);
        if (!this.outbound.offer(request)) {
            this.pending.remove(correlationId);
            throw new IOException("Shardingbase proxy outbound queue is full");
        }
        try {
            return response.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for the Shardingbase proxy", exception);
        } catch (final TimeoutException exception) {
            throw new IOException("Shardingbase proxy request timed out", exception);
        } catch (final ExecutionException exception) {
            final Throwable cause = exception.getCause();
            if (cause instanceof final IOException ioException) {
                throw ioException;
            }
            throw new IOException("Shardingbase proxy request failed", cause);
        } finally {
            this.pending.remove(correlationId);
        }
    }

    void pushHandler(final Consumer<ProtocolFrame> pushHandler) {
        this.pushHandler = Objects.requireNonNull(pushHandler, "pushHandler");
    }

    void respond(final ProtocolFrame request, final MessageType messageType, final byte[] payload) throws IOException {
        if (this.connection.get() == null) {
            throw new IOException("Shardingbase proxy session is not connected");
        }
        final ProtocolFrame response = new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            request.channel(),
            messageType,
            request.correlationId(),
            this.nodeId(),
            request.sourceId(),
            payload
        );
        if (!this.outbound.offer(response)) {
            throw new IOException("Shardingbase proxy outbound queue is full");
        }
    }

    boolean connected() {
        return this.connection.get() != null;
    }

    private void connectLoop() {
        int attempt = 0;
        while (!this.closed.get()) {
            try {
                this.runConnection();
                attempt = 0;
            } catch (final IOException exception) {
                if (!this.closed.get()) {
                    System.err.println("Shardingbase proxy session disconnected: " + exception.getMessage());
                }
            } finally {
                this.disconnect(new IOException("Shardingbase proxy session disconnected"));
            }
            if (!this.closed.get()) {
                final long delaySeconds = Math.min(60, 1L << Math.min(attempt++, 6));
                try {
                    TimeUnit.SECONDS.sleep(delaySeconds);
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void runConnection() throws IOException {
        final URI uri = controllerUri(this.environment.get(CONTROLLER_URI_ENVIRONMENT_VARIABLE));
        final String fingerprint = normalizeFingerprint(
            this.environment.get(CERTIFICATE_FINGERPRINT_ENVIRONMENT_VARIABLE)
        );
        if (fingerprint == null) {
            throw new IOException("Invalid Shardingbase controller certificate fingerprint");
        }
        final int timeoutMillis = Math.toIntExact(REQUEST_TIMEOUT.toMillis());
        final SSLSocket socket = (SSLSocket) sslContext().getSocketFactory().createSocket();
        try {
            socket.setEnabledProtocols(new String[] {"TLSv1.3"});
            socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), timeoutMillis);
            socket.setSoTimeout(15_000);
            socket.startHandshake();
            verifyFingerprint(socket, fingerprint);
            this.authenticate(socket);
            final ActiveConnection active = new ActiveConnection(socket);
            this.connection.set(active);
            final Thread writer = Thread.ofPlatform()
                .daemon(true)
                .name("Shardingbase Proxy Writer")
                .start(() -> this.writeLoop(active));
            try {
                while (!this.closed.get() && this.connection.get() == active) {
                    final ProtocolFrame frame = FrameCodec.read(socket.getInputStream());
                    final CompletableFuture<ProtocolFrame> response = this.pending.remove(frame.correlationId());
                    if (response != null) {
                        response.complete(frame);
                    } else {
                        this.pushHandler.accept(frame);
                    }
                }
            } finally {
                this.connection.compareAndSet(active, null);
                writer.interrupt();
            }
        } finally {
            socket.close();
        }
    }

    private void authenticate(final SSLSocket socket) throws IOException {
        final UUID correlationId = UUID.randomUUID();
        FrameCodec.write(socket.getOutputStream(), new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            ProtocolChannel.CONTROL,
            MessageType.AUTHENTICATE_NODE_REQUEST,
            correlationId,
            this.nodeId(),
            "velocity",
            NodeAuthenticationCodec.encodeRequest(this.environment.get(CREDENTIAL_ENVIRONMENT_VARIABLE))
        ));
        final ProtocolFrame response = FrameCodec.read(socket.getInputStream());
        if (!correlationId.equals(response.correlationId())
            || response.messageType() != MessageType.AUTHENTICATE_NODE_RESPONSE) {
            throw new IOException("Velocity returned an unrelated node authentication response");
        }
        final AuthenticationResponse authentication = NodeAuthenticationCodec.decodeResponse(response.payload());
        if (!authentication.accepted()) {
            throw new IOException("Velocity rejected node authentication: " + authentication.detail());
        }
    }

    private void writeLoop(final ActiveConnection active) {
        try {
            while (!this.closed.get() && this.connection.get() == active) {
                final ProtocolFrame frame = this.outbound.poll(1, TimeUnit.SECONDS);
                if (frame != null) {
                    active.write(frame);
                }
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (IOException _) {
            active.close();
        }
    }

    private void heartbeat() {
        if (this.connection.get() == null || this.closed.get()) {
            return;
        }
        try {
            final ProtocolFrame response = this.request(
                ProtocolChannel.CONTROL,
                MessageType.HEARTBEAT,
                "velocity",
                new byte[0]
            );
            if (response.messageType() != MessageType.HEARTBEAT_ACK) {
                throw new IOException("Unexpected heartbeat response");
            }
        } catch (IOException _) {
            final ActiveConnection active = this.connection.getAndSet(null);
            if (active != null) {
                active.close();
            }
        }
    }

    private void disconnect(final IOException failure) {
        final ActiveConnection active = this.connection.getAndSet(null);
        if (active != null) {
            active.close();
        }
        this.outbound.clear();
        this.pending.forEach((correlationId, future) -> future.completeExceptionally(failure));
        this.pending.clear();
    }

    private boolean configurationAvailable() {
        return nonBlank(this.environment.get(CONTROLLER_URI_ENVIRONMENT_VARIABLE))
            && nonBlank(this.environment.get(CERTIFICATE_FINGERPRINT_ENVIRONMENT_VARIABLE))
            && nonBlank(this.environment.get(CREDENTIAL_ENVIRONMENT_VARIABLE))
            && nonBlank(this.environment.get(NODE_ID_ENVIRONMENT_VARIABLE));
    }

    private String nodeId() {
        return this.environment.get(NODE_ID_ENVIRONMENT_VARIABLE);
    }

    private static boolean nonBlank(final String value) {
        return value != null && !value.isBlank();
    }

    private static URI controllerUri(final String controller) throws IOException {
        final URI uri;
        try {
            uri = URI.create(controller);
        } catch (final IllegalArgumentException exception) {
            throw new IOException("Invalid Shardingbase controller URI", exception);
        }
        if (!"tls".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getPort() < 1) {
            throw new IOException("Controller URI must use tls and include an explicit port");
        }
        return uri;
    }

    private static SSLContext sslContext() throws IOException {
        try {
            final SSLContext context = SSLContext.getInstance("TLSv1.3");
            context.init(null, new TrustManager[] {new PinningTrustManager()}, new SecureRandom());
            return context;
        } catch (final GeneralSecurityException exception) {
            throw new IOException("Unable to initialize TLS", exception);
        }
    }

    private static void verifyFingerprint(final SSLSocket socket, final String expected) throws IOException {
        try {
            final X509Certificate certificate = (X509Certificate) socket.getSession().getPeerCertificates()[0];
            final String actual = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded())
            ).toUpperCase(Locale.ROOT);
            if (!MessageDigest.isEqual(actual.getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                expected.getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
                throw new IOException("Velocity TLS certificate fingerprint mismatch; received " + actual);
            }
        } catch (final GeneralSecurityException exception) {
            throw new IOException("Unable to verify Velocity TLS certificate", exception);
        }
    }

    private static String normalizeFingerprint(final String fingerprint) {
        if (fingerprint == null) {
            return null;
        }
        final String normalized = fingerprint.replace(":", "").replace(" ", "").toUpperCase(Locale.ROOT);
        return normalized.matches("[0-9A-F]{64}") ? normalized : null;
    }

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        this.disconnect(new IOException("Shardingbase node is shutting down"));
        this.heartbeatExecutor.shutdownNow();
        this.connectionExecutor.shutdownNow();
    }

    private static final class ActiveConnection {
        private final SSLSocket socket;

        private ActiveConnection(final SSLSocket socket) {
            this.socket = socket;
        }

        private synchronized void write(final ProtocolFrame frame) throws IOException {
            FrameCodec.write(this.socket.getOutputStream(), frame);
        }

        private void close() {
            try {
                this.socket.close();
            } catch (IOException _) {
            }
        }
    }

    private static final class PinningTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authenticationType) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authenticationType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
