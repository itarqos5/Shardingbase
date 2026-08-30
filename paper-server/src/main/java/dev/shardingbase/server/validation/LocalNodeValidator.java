package dev.shardingbase.server.validation;

import dev.shardingbase.api.ServerIdentity;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/** Authenticated loopback validation client used by a backend child process. */
public final class LocalNodeValidator implements BackendValidator {
    /** Child-process environment variable containing the node's loopback port. */
    public static final String PORT_ENVIRONMENT_VARIABLE = "SHARDINGBASE_NODE_PORT";
    /** Child-process environment variable containing the ephemeral node credential. */
    public static final String TOKEN_ENVIRONMENT_VARIABLE = "SHARDINGBASE_NODE_TOKEN";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final Map<String, String> environment;
    private final String minecraftVersion;
    private final String shardingbaseVersion;

    /**
     * Creates a validator using the process environment.
     *
     * @param minecraftVersion running Minecraft version
     * @param shardingbaseVersion running Shardingbase build version
     */
    public LocalNodeValidator(final String minecraftVersion, final String shardingbaseVersion) {
        this(System.getenv(), minecraftVersion, shardingbaseVersion);
    }

    LocalNodeValidator(
        final Map<String, String> environment,
        final String minecraftVersion,
        final String shardingbaseVersion
    ) {
        this.environment = Map.copyOf(environment);
        this.minecraftVersion = minecraftVersion;
        this.shardingbaseVersion = shardingbaseVersion;
    }

    @Override
    public ValidationResult validate(final ServerIdentity identity) throws IOException {
        final String portValue = this.environment.get(PORT_ENVIRONMENT_VARIABLE);
        final String token = this.environment.get(TOKEN_ENVIRONMENT_VARIABLE);
        if (portValue == null || token == null || token.isBlank()) {
            return new ValidationResult(false, "local Shardingbase node is not available", "", "");
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

        final int timeoutMillis = Math.toIntExact(TIMEOUT.toMillis());
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            try (
                DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()))
            ) {
                output.writeInt(ShardingbaseProtocol.MAGIC);
                output.writeInt(ShardingbaseProtocol.VERSION);
                writeField(output, token);
                writeField(output, identity.serverId());
                writeField(output, identity.serverName());
                writeField(output, this.minecraftVersion);
                writeField(output, this.shardingbaseVersion);
                output.flush();

                if (input.readInt() != ShardingbaseProtocol.MAGIC) {
                    throw new IOException("Local node returned an invalid response marker");
                }
                final int version = input.readInt();
                if (version != ShardingbaseProtocol.VERSION) {
                    return new ValidationResult(false, "node protocol mismatch: " + version, "", "");
                }
                final boolean accepted = input.readBoolean();
                return new ValidationResult(accepted, readField(input), readField(input), readField(input));
            }
        }
    }

    private static void writeField(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new IOException("Control field exceeds " + ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES + " bytes");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readField(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new IOException("Invalid control field length: " + length);
        }
        final byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("Local node response ended inside a control field");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
