package dev.shardingbase.node;

import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.protocol.ValidationPayloadCodec;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationRequest;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Authenticated loopback service exposed only to the backend child. */
final class LocalBackendController implements AutoCloseable {
    private static final int CLIENT_TIMEOUT_MILLIS = 12_000;

    private final ServerSocket serverSocket;
    private final String token;
    private final ProxyValidationClient proxyClient;
    private final ExecutorService clients;
    private final Semaphore clientCapacity = new Semaphore(32);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<ProtocolChannel, ArrayBlockingQueue<ProtocolFrame>> backendPushes =
        new ConcurrentHashMap<>();
    private final Thread acceptThread;
    private volatile LocalRequestHandler localRequestHandler;

    LocalBackendController(final ProxyValidationClient proxyClient) throws IOException {
        this.proxyClient = proxyClient;
        this.serverSocket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
        final byte[] tokenBytes = new byte[32];
        new SecureRandom().nextBytes(tokenBytes);
        this.token = Base64.getUrlEncoder().withoutPadding().encodeToString(tokenBytes);
        this.clients = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
            .name("Shardingbase Local Control Client")
            .factory());
        this.acceptThread = Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Local Control")
            .start(this::acceptLoop);
        for (final ProtocolChannel channel : new ProtocolChannel[] {
            ProtocolChannel.PLAYER_SYNC,
            ProtocolChannel.COMMAND,
            ProtocolChannel.REMOTE_OPERATION,
            ProtocolChannel.MAP,
            ProtocolChannel.WORLD_TRANSACTION
        }) {
            this.backendPushes.put(channel, new ArrayBlockingQueue<>(64));
            if (channel != ProtocolChannel.WORLD_TRANSACTION) {
                this.proxyClient.pushHandler(channel, this::enqueueProxyPush);
            }
        }
    }

    static LocalBackendController start(final ProxyValidationClient proxyClient) throws IOException {
        return new LocalBackendController(proxyClient);
    }

    Map<String, String> childEnvironment() {
        return Map.of(
            "SHARDINGBASE_NODE_PORT", Integer.toString(this.serverSocket.getLocalPort()),
            "SHARDINGBASE_NODE_TOKEN", this.token
        );
    }

    void localRequestHandler(final LocalRequestHandler handler) {
        this.localRequestHandler = handler;
    }

    void enqueueBackendPush(final ProtocolFrame frame) throws IOException {
        final ArrayBlockingQueue<ProtocolFrame> queue = this.backendPushes.get(frame.channel());
        if (queue == null || !queue.offer(frame)) {
            throw new IOException("backend control queue is full for " + frame.channel());
        }
    }

    private void acceptLoop() {
        while (!this.closed.get()) {
            try {
                final Socket socket = this.serverSocket.accept();
                if (!this.clientCapacity.tryAcquire()) {
                    socket.close();
                    continue;
                }
                try {
                    this.clients.execute(() -> {
                        try {
                            this.handle(socket);
                        } finally {
                            this.clientCapacity.release();
                        }
                    });
                } catch (final RuntimeException exception) {
                    this.clientCapacity.release();
                    socket.close();
                    throw exception;
                }
            } catch (final IOException exception) {
                if (!this.closed.get()) {
                    System.err.println("Shardingbase local control accept failed: " + exception.getMessage());
                }
            }
        }
    }

    private void handle(final Socket socket) {
        try (socket) {
            socket.setSoTimeout(CLIENT_TIMEOUT_MILLIS);
            try (
                DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream())
            ) {
                if (input.readInt() != ShardingbaseProtocol.MAGIC) {
                    return;
                }
                final int version = input.readInt();
                final String presentedToken = readField(input);
                if (!constantTimeEquals(this.token, presentedToken)) {
                    throw new IOException("local node authentication failed");
                }
                if (version != ShardingbaseProtocol.VERSION) {
                    throw new IOException("backend/node protocol mismatch");
                }
                final ProtocolFrame request = FrameCodec.read(input);
                if (request.version() != ShardingbaseProtocol.VERSION) {
                    throw new IOException("backend frame protocol mismatch");
                }
                final ProtocolFrame proxyResponse = this.forward(request);
                final ProtocolFrame response = new ProtocolFrame(
                    ShardingbaseProtocol.VERSION,
                    proxyResponse.channel(),
                    proxyResponse.messageType(),
                    request.correlationId(),
                    "node",
                    request.sourceId(),
                    proxyResponse.payload()
                );
                FrameCodec.write(output, response);
            }
        } catch (final IOException exception) {
            if (!this.closed.get()) {
                System.err.println("Shardingbase local control request failed: " + exception.getMessage());
            }
        }
    }

    private ProtocolFrame forward(final ProtocolFrame request) throws IOException {
        if ("node-local".equals(request.targetId())) {
            if (request.messageType() == MessageType.BACKEND_POLL) {
                final ArrayBlockingQueue<ProtocolFrame> queue = this.backendPushes.get(request.channel());
                if (queue == null) {
                    throw new IOException("Unsupported backend polling channel " + request.channel());
                }
                final ProtocolFrame pushed;
                try {
                    pushed = queue.poll(5, TimeUnit.SECONDS);
                } catch (final InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while polling backend control messages", exception);
                }
                if (pushed != null) {
                    return pushed;
                }
                return new ProtocolFrame(
                    ShardingbaseProtocol.VERSION,
                    request.channel(),
                    MessageType.BACKEND_POLL_EMPTY,
                    request.correlationId(),
                    "node",
                    request.sourceId(),
                    new byte[0]
                );
            }
            final LocalRequestHandler handler = this.localRequestHandler;
            if (handler == null) {
                throw new IOException("No local node handler is registered for " + request.channel());
            }
            return handler.handle(request);
        }
        if (!"velocity".equals(request.targetId())) {
            this.proxyClient.send(
                request.channel(),
                request.messageType(),
                request.targetId(),
                request.payload()
            );
            return new ProtocolFrame(
                ShardingbaseProtocol.VERSION,
                request.channel(),
                MessageType.BACKEND_SEND_ACK,
                request.correlationId(),
                "node",
                request.sourceId(),
                new byte[0]
            );
        }
        if (request.messageType() == MessageType.VALIDATE_BACKEND_REQUEST) {
            final ValidationRequest validation = ValidationPayloadCodec.decodeRequest(request.payload());
            final ValidationResponse response = this.proxyClient.validate(
                validation.serverId(),
                validation.serverName(),
                validation.minecraftVersion(),
                validation.shardingbaseVersion()
            );
            return new ProtocolFrame(
                ShardingbaseProtocol.VERSION,
                request.channel(),
                MessageType.VALIDATE_BACKEND_RESPONSE,
                request.correlationId(),
                "velocity",
                request.sourceId(),
                ValidationPayloadCodec.encodeResponse(response)
            );
        }
        return this.proxyClient.request(
            request.channel(),
            request.messageType(),
            request.targetId(),
            request.payload()
        );
    }

    private void enqueueProxyPush(final ProtocolFrame frame) {
        try {
            this.enqueueBackendPush(frame);
            return;
        } catch (IOException _) {
        }
        try {
            this.proxyClient.respond(
                frame,
                MessageType.ERROR,
                "backend control queue is full".getBytes(StandardCharsets.UTF_8)
            );
        } catch (IOException _) {
        }
    }

    private static boolean constantTimeEquals(final String expected, final String actual) {
        return MessageDigest.isEqual(
            expected.getBytes(StandardCharsets.UTF_8),
            actual.getBytes(StandardCharsets.UTF_8)
        );
    }

    private static String readField(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new IOException("Invalid local control field length: " + length);
        }
        final byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("Local control request ended inside a field");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        this.serverSocket.close();
        this.clients.shutdownNow();
        this.acceptThread.interrupt();
    }

    @FunctionalInterface
    interface LocalRequestHandler {
        ProtocolFrame handle(ProtocolFrame request) throws IOException;
    }
}
