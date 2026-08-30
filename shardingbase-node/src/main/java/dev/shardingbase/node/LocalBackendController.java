package dev.shardingbase.node;

import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** Authenticated loopback service exposed only to the backend child. */
final class LocalBackendController implements AutoCloseable {
    private static final int CLIENT_TIMEOUT_MILLIS = 12_000;

    private final ServerSocket serverSocket;
    private final String token;
    private final ProxyValidationClient proxyClient;
    private final ExecutorService clients;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread acceptThread;

    LocalBackendController(final ProxyValidationClient proxyClient) throws IOException {
        this.proxyClient = proxyClient;
        this.serverSocket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
        final byte[] tokenBytes = new byte[32];
        new SecureRandom().nextBytes(tokenBytes);
        this.token = Base64.getUrlEncoder().withoutPadding().encodeToString(tokenBytes);
        this.clients = Executors.newFixedThreadPool(2, task -> Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Local Control Client")
            .unstarted(task));
        this.acceptThread = Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Local Control")
            .start(this::acceptLoop);
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

    private void acceptLoop() {
        while (!this.closed.get()) {
            try {
                final Socket socket = this.serverSocket.accept();
                this.clients.execute(() -> this.handle(socket));
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
                DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))
            ) {
                if (input.readInt() != ShardingbaseProtocol.MAGIC) {
                    return;
                }
                final int version = input.readInt();
                final String presentedToken = readField(input);
                final String serverId = readField(input);
                final String serverName = readField(input);
                final String minecraftVersion = readField(input);
                final String shardingbaseVersion = readField(input);

                final ValidationResponse response;
                if (!constantTimeEquals(this.token, presentedToken)) {
                    response = new ValidationResponse(false, "local node authentication failed", "", "");
                } else if (version != ShardingbaseProtocol.VERSION) {
                    response = new ValidationResponse(false, "backend/node protocol mismatch", "", "");
                } else {
                    response = this.proxyClient.validate(serverId, serverName, minecraftVersion, shardingbaseVersion);
                }
                output.writeInt(ShardingbaseProtocol.MAGIC);
                output.writeInt(ShardingbaseProtocol.VERSION);
                output.writeBoolean(response.accepted());
                writeField(output, response.detail());
                writeField(output, response.peerId());
                writeField(output, response.peerName());
                output.flush();
            }
        } catch (final IOException exception) {
            if (!this.closed.get()) {
                System.err.println("Shardingbase local validation failed: " + exception.getMessage());
            }
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

    private static void writeField(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new IOException("Local control response exceeds the field limit");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
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
