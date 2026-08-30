package dev.shardingbase.velocity;

import com.velocitypowered.api.proxy.ProxyServer;
import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ReplayWindow;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLServerSocket;
import org.slf4j.Logger;

/** Bounded TLS control listener for node validation requests. */
final class ControlServer implements AutoCloseable {
    private static final int CLIENT_TIMEOUT_MILLIS = 12_000;

    private final ProxyServer proxy;
    private final Logger logger;
    private final Map<String, String> credentials;
    private final BackendRegistry registry;
    private final ReplayWindow replayWindow = new ReplayWindow(16_384, Duration.ofMinutes(15));
    private final SSLServerSocket serverSocket;
    private final ThreadPoolExecutor clients;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread acceptThread;

    ControlServer(
        final ProxyServer proxy,
        final Logger logger,
        final VelocityConfiguration configuration,
        final TlsMaterial tlsMaterial,
        final BackendRegistry registry
    ) throws IOException {
        this.proxy = proxy;
        this.logger = logger;
        this.credentials = configuration.nodeCredentials();
        this.registry = registry;
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
        try (socket) {
            socket.setSoTimeout(CLIENT_TIMEOUT_MILLIS);
            final ProtocolFrame requestFrame = FrameCodec.read(socket.getInputStream());
            final ValidationResponse response = this.validate(requestFrame);
            FrameCodec.write(socket.getOutputStream(), new ProtocolFrame(
                ShardingbaseProtocol.VERSION,
                ProtocolChannel.CONTROL,
                MessageType.VALIDATE_BACKEND_RESPONSE,
                requestFrame.correlationId(),
                "velocity",
                requestFrame.sourceId(),
                ValidationPayloadCodec.encodeResponse(response)
            ));
        } catch (final IOException exception) {
            if (!this.closed.get()) {
                this.logger.warn("Rejected Shardingbase control request: {}", exception.getMessage());
            }
        }
    }

    private ValidationResponse validate(final ProtocolFrame frame) throws IOException {
        if (frame.version() != ShardingbaseProtocol.VERSION) {
            return rejected("protocol version mismatch");
        }
        if (frame.channel() != ProtocolChannel.CONTROL || frame.messageType() != MessageType.VALIDATE_BACKEND_REQUEST) {
            return rejected("unexpected control message");
        }
        if (!this.replayWindow.accept(frame.correlationId())) {
            return rejected("replayed correlation ID");
        }
        final ValidationRequest request = ValidationPayloadCodec.decodeRequest(frame.payload());
        final String expectedCredential = this.credentials.get(frame.sourceId());
        if (expectedCredential == null || !constantTimeEquals(expectedCredential, request.credential())) {
            return rejected("node authentication failed");
        }
        if (this.proxy.getServer(request.serverName()).isEmpty()) {
            return rejected("server-name is not present in Velocity's servers configuration");
        }
        return this.registry.register(frame.sourceId(), request);
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
        this.clients.shutdownNow();
        this.acceptThread.interrupt();
    }
}
