package dev.shardingbase.velocity;

import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.NodeAuthenticationCodec;
import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.protocol.ValidationPayloadCodec;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.helpers.NOPLogger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistentControlServerTest {
    @Test
    void authenticatesAndHandlesMultipleCorrelatedRequestsOnOneTlsSession(@TempDir final Path directory)
        throws Exception {
        final int port = freePort();
        final VelocityConfiguration configuration = new VelocityConfiguration(
            "127.0.0.1",
            port,
            directory.resolve("tls.p12"),
            "test-password",
            directory.resolve("shardingbase.db"),
            Map.of("node-a", "credential-a", "node-b", "credential-b")
        );
        final TlsMaterial tls = TlsMaterial.loadOrCreate(configuration);
        final BackendRegistry registry = new BackendRegistry(configuration.databasePath());
        registry.register("node-a", validationRequest("credential-a", "backend-id-a", "backend-a"));
        registry.register("node-b", validationRequest("credential-b", "backend-id-b", "backend-b"));
        final PlayerStateStore playerStateStore = new PlayerStateStore(configuration.databasePath());
        try (ControlServer server = new ControlServer(
            proxyWithBackend(),
            NOPLogger.NOP_LOGGER,
            configuration,
            tls,
            registry,
            playerStateStore
        ); SSLSocket socket = connect(port)) {
            final UUID authenticationId = UUID.randomUUID();
            FrameCodec.write(socket.getOutputStream(), frame(
                MessageType.AUTHENTICATE_NODE_REQUEST,
                authenticationId,
                NodeAuthenticationCodec.encodeRequest("credential-a")
            ));
            final ProtocolFrame authentication = FrameCodec.read(socket.getInputStream());
            assertEquals(authenticationId, authentication.correlationId());
            assertTrue(NodeAuthenticationCodec.decodeResponse(authentication.payload()).accepted());

            for (int request = 0; request < 2; request++) {
                final UUID heartbeatId = UUID.randomUUID();
                FrameCodec.write(socket.getOutputStream(), frame(MessageType.HEARTBEAT, heartbeatId, new byte[0]));
                final ProtocolFrame heartbeat = FrameCodec.read(socket.getInputStream());
                assertEquals(MessageType.HEARTBEAT_ACK, heartbeat.messageType());
                assertEquals(heartbeatId, heartbeat.correlationId());
            }

            final UUID validationId = UUID.randomUUID();
            FrameCodec.write(socket.getOutputStream(), frame(
                MessageType.VALIDATE_BACKEND_REQUEST,
                validationId,
                ValidationPayloadCodec.encodeRequest(validationRequest("credential-a", "backend-id-a", "backend-a"))
            ));
            final ProtocolFrame validation = FrameCodec.read(socket.getInputStream());
            assertEquals(MessageType.VALIDATE_BACKEND_RESPONSE, validation.messageType());
            assertEquals(validationId, validation.correlationId());

            final UUID playerId = UUID.randomUUID();
            final UUID prepareId = UUID.randomUUID();
            FrameCodec.write(socket.getOutputStream(), frame(
                MessageType.PLAYER_SNAPSHOT_PREPARE,
                prepareId,
                PlayerHandoffCodec.encodePrepare(new PlayerHandoffCodec.Prepare(
                    playerId,
                    "backend-id-b",
                    EnumSet.of(PlayerDataCategory.INVENTORY)
                ))
            ));
            final PlayerHandoffCodec.Acknowledgement prepared = PlayerHandoffCodec.decodeAcknowledgement(
                FrameCodec.read(socket.getInputStream()).payload()
            );
            assertTrue(prepared.accepted());

            final UUID stageId = UUID.randomUUID();
            FrameCodec.write(socket.getOutputStream(), frame(
                MessageType.PLAYER_SNAPSHOT_STAGE,
                stageId,
                PlayerHandoffCodec.encodeStage(new PlayerHandoffCodec.Stage(
                    "backend-id-b",
                    new PlayerSnapshot(
                        playerId,
                        prepared.revision(),
                        "backend-id-a",
                        Map.of(PlayerDataCategory.INVENTORY, new byte[] {1, 2})
                    )
                ))
            ));
            final PlayerHandoffCodec.Acknowledgement staged = PlayerHandoffCodec.decodeAcknowledgement(
                FrameCodec.read(socket.getInputStream()).payload()
            );
            assertTrue(staged.accepted());
            assertEquals(prepared.revision(), playerStateStore.load(playerId).orElseThrow().revision());

            try (SSLSocket targetSocket = connect(port)) {
                final UUID targetAuthenticationId = UUID.randomUUID();
                FrameCodec.write(targetSocket.getOutputStream(), frame(
                    "node-b",
                    MessageType.AUTHENTICATE_NODE_REQUEST,
                    targetAuthenticationId,
                    NodeAuthenticationCodec.encodeRequest("credential-b")
                ));
                assertTrue(NodeAuthenticationCodec.decodeResponse(
                    FrameCodec.read(targetSocket.getInputStream()).payload()
                ).accepted());

                final UUID fetchId = UUID.randomUUID();
                FrameCodec.write(targetSocket.getOutputStream(), frame(
                    "node-b",
                    MessageType.PLAYER_SNAPSHOT_FETCH,
                    fetchId,
                    PlayerHandoffCodec.encodeFetch(new PlayerHandoffCodec.Fetch(playerId, "backend-id-b"))
                ));
                final ProtocolFrame fetchedFrame = FrameCodec.read(targetSocket.getInputStream());
                assertEquals(MessageType.PLAYER_SNAPSHOT_FETCH_RESPONSE, fetchedFrame.messageType());
                final PlayerHandoffCodec.FetchResponse fetched = PlayerHandoffCodec.decodeFetchResponse(
                    fetchedFrame.payload()
                );
                assertEquals(prepared.revision(), fetched.stage().snapshot().revision());
            }
        }
    }

    private static ValidationPayloadCodec.ValidationRequest validationRequest(
        final String credential,
        final String serverId,
        final String serverName
    ) {
        return new ValidationPayloadCodec.ValidationRequest(credential, serverId, serverName, "26.2", "test-build");
    }

    private static ProtocolFrame frame(final MessageType type, final UUID correlationId, final byte[] payload) {
        return frame("node-a", type, correlationId, payload);
    }

    private static ProtocolFrame frame(
        final String sourceId,
        final MessageType type,
        final UUID correlationId,
        final byte[] payload
    ) {
        return new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            ProtocolChannel.CONTROL,
            type,
            correlationId,
            sourceId,
            "velocity",
            payload
        );
    }

    private static SSLSocket connect(final int port) throws Exception {
        final SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(null, new TrustManager[] {new TrustAllManager()}, new SecureRandom());
        final SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket();
        socket.setEnabledProtocols(new String[] {"TLSv1.3"});
        socket.connect(new InetSocketAddress("127.0.0.1", port), 10_000);
        socket.setSoTimeout(10_000);
        socket.startHandshake();
        return socket;
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static ProxyServer proxyWithBackend() {
        final RegisteredServer registered = (RegisteredServer) Proxy.newProxyInstance(
            RegisteredServer.class.getClassLoader(),
            new Class<?>[] {RegisteredServer.class},
            (proxy, method, arguments) -> defaultValue(proxy, method.getName(), method.getReturnType(), arguments)
        );
        return (ProxyServer) Proxy.newProxyInstance(
            ProxyServer.class.getClassLoader(),
            new Class<?>[] {ProxyServer.class},
            (proxy, method, arguments) -> "getServer".equals(method.getName())
                ? Optional.of(registered)
                : defaultValue(proxy, method.getName(), method.getReturnType(), arguments)
        );
    }

    private static Object defaultValue(
        final Object proxy,
        final String method,
        final Class<?> returnType,
        final Object[] arguments
    ) {
        return switch (method) {
            case "toString" -> "ShardingbaseTestProxy";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == arguments[0];
            default -> returnType == boolean.class ? false : returnType == int.class ? 0 : null;
        };
    }

    private static final class TrustAllManager implements X509TrustManager {
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
