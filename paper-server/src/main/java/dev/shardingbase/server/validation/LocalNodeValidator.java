package dev.shardingbase.server.validation;

import dev.shardingbase.api.ServerIdentity;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ValidationPayloadCodec;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationRequest;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationResponse;
import java.io.IOException;
import java.util.Map;

/** Authenticated loopback validation client used by a backend child process. */
public final class LocalNodeValidator implements BackendValidator {
    /** Child-process environment variable containing the node's loopback port. */
    public static final String PORT_ENVIRONMENT_VARIABLE = LocalNodeClient.PORT_ENVIRONMENT_VARIABLE;
    /** Child-process environment variable containing the ephemeral node credential. */
    public static final String TOKEN_ENVIRONMENT_VARIABLE = LocalNodeClient.TOKEN_ENVIRONMENT_VARIABLE;

    private final LocalNodeClient client;
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
        this.client = new LocalNodeClient(environment);
        this.minecraftVersion = minecraftVersion;
        this.shardingbaseVersion = shardingbaseVersion;
    }

    @Override
    public ValidationResult validate(final ServerIdentity identity) throws IOException {
        if (!this.client.available()) {
            return new ValidationResult(false, "local Shardingbase node is not available", "", "");
        }
        final ValidationRequest request = new ValidationRequest(
            "",
            identity.serverId(),
            identity.serverName(),
            this.minecraftVersion,
            this.shardingbaseVersion
        );
        final ProtocolFrame frame = this.client.request(
            identity.serverId(),
            ProtocolChannel.CONTROL,
            MessageType.VALIDATE_BACKEND_REQUEST,
            "velocity",
            ValidationPayloadCodec.encodeRequest(request)
        );
        if (frame.messageType() != MessageType.VALIDATE_BACKEND_RESPONSE) {
            throw new IOException("Local node returned an unexpected validation response");
        }
        final ValidationResponse response = ValidationPayloadCodec.decodeResponse(frame.payload());
        return new ValidationResult(response.accepted(), response.detail(), response.peerId(), response.peerName());
    }
}
