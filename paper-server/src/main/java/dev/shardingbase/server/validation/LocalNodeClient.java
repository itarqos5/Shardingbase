package dev.shardingbase.server.validation;

import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/** Authenticated framed client for the node supervising this backend process. */
public final class LocalNodeClient {
    /** Child-process environment variable containing the node's loopback port. */
    public static final String PORT_ENVIRONMENT_VARIABLE = "SHARDINGBASE_NODE_PORT";
    /** Child-process environment variable containing the ephemeral node credential. */
    public static final String TOKEN_ENVIRONMENT_VARIABLE = "SHARDINGBASE_NODE_TOKEN";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final Map<String, String> environment;

    /** Creates a client using the process environment. */
    public LocalNodeClient() {
        this(System.getenv());
    }

    LocalNodeClient(final Map<String, String> environment) {
        this.environment = Map.copyOf(environment);
    }

    /**
     * Sends one request through the local node's persistent authenticated proxy session.
     *
     * @param sourceId backend server ID
     * @param channel protocol channel
     * @param messageType request type
     * @param targetId logical target
     * @param payload encoded request payload
     * @return correlated response from the target
     * @throws IOException if local authentication, transport, or proxy routing fails
     */
    public ProtocolFrame request(
        final String sourceId,
        final ProtocolChannel channel,
        final MessageType messageType,
        final String targetId,
        final byte[] payload
    ) throws IOException {
        final Endpoint endpoint = this.endpoint();
        final UUID correlationId = UUID.randomUUID();
        final ProtocolFrame request = new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            channel,
            messageType,
            correlationId,
            sourceId,
            targetId,
            payload
        );
        final int timeoutMillis = Math.toIntExact(TIMEOUT.toMillis());
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), endpoint.port()), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            try (
                DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()))
            ) {
                output.writeInt(ShardingbaseProtocol.MAGIC);
                output.writeInt(ShardingbaseProtocol.VERSION);
                writeField(output, endpoint.token());
                FrameCodec.write(output, request);

                final ProtocolFrame response = FrameCodec.read(input);
                if (!correlationId.equals(response.correlationId())) {
                    throw new IOException("Local node returned an unrelated response");
                }
                if (response.version() != ShardingbaseProtocol.VERSION) {
                    throw new IOException("Local node returned protocol version " + response.version());
                }
                if (response.messageType() == MessageType.ERROR) {
                    throw new IOException(new String(response.payload(), StandardCharsets.UTF_8));
                }
                return response;
            }
        }
    }

    /** Returns whether the backend was launched with usable local-node credentials. */
    public boolean available() {
        try {
            this.endpoint();
            return true;
        } catch (IOException _) {
            return false;
        }
    }

    private Endpoint endpoint() throws IOException {
        final String portValue = this.environment.get(PORT_ENVIRONMENT_VARIABLE);
        final String token = this.environment.get(TOKEN_ENVIRONMENT_VARIABLE);
        if (portValue == null || token == null || token.isBlank()) {
            throw new IOException("local Shardingbase node is not available");
        }
        final int port;
        try {
            port = Integer.parseInt(portValue);
        } catch (final NumberFormatException exception) {
            throw new IOException(PORT_ENVIRONMENT_VARIABLE + " is not a valid port", exception);
        }
        if (port < 1 || port > 65_535) {
            throw new IOException(PORT_ENVIRONMENT_VARIABLE + " is outside the valid port range");
        }
        return new Endpoint(port, token);
    }

    private static void writeField(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new IOException("Control field exceeds " + ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES + " bytes");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private record Endpoint(int port, String token) {
    }
}
