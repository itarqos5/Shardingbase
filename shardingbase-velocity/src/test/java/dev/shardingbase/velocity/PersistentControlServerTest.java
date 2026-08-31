package dev.shardingbase.velocity;

import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.NodeAuthenticationCodec;
import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSettingsCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.RemoteCommandCodec;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.protocol.ValidationPayloadCodec;
import dev.shardingbase.protocol.WorldTransactionCodec;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.helpers.NOPLogger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
            Map.of("node-a", "credential-a", "node-b", "credential-b"),
            Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]),
            Set.of("home"),
            "127.0.0.1",
            8080,
            "http://127.0.0.1:8080",
            false
        );
        final TlsMaterial tls = TlsMaterial.loadOrCreate(configuration);
        final BackendRegistry registry = new BackendRegistry(configuration.databasePath());
        registry.register("node-a", validationRequest("credential-a", "backend-id-a", "backend-a"));
        registry.register("node-b", validationRequest("credential-b", "backend-id-b", "backend-b"));
        final PlayerStateStore playerStateStore = new PlayerStateStore(configuration.databasePath());
        final WorldPlannerStore worldPlannerStore = new WorldPlannerStore(configuration.databasePath());
        try (ControlServer server = new ControlServer(
            proxyWithBackend(),
            NOPLogger.NOP_LOGGER,
            configuration,
            tls,
            registry,
            playerStateStore,
            worldPlannerStore
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

            final UUID pushedPlayerId = UUID.randomUUID();
            server.sendPlayerCapture(
                registry.backendForName("backend-a").orElseThrow(),
                new PlayerHandoffCodec.Capture(
                    pushedPlayerId,
                    "backend-id-b",
                    3,
                    EnumSet.of(PlayerDataCategory.INVENTORY)
                )
            );
            final ProtocolFrame pushed = FrameCodec.read(socket.getInputStream());
            assertEquals(MessageType.PLAYER_SNAPSHOT_CAPTURE, pushed.messageType());
            assertEquals(3, PlayerHandoffCodec.decodeCapture(pushed.payload()).revision());

            final PlayerHandoffCodec.TransferDestination destination =
                new PlayerHandoffCodec.TransferDestination(
                    "minecraft:overworld", UUID.randomUUID(), 8.5, 72.0, -3.25, 45.0F, 2.0F
                );
            server.boundaryTransferHandler(request -> {
                assertEquals("backend-id-a", request.sourceBackendId());
                assertEquals(destination, request.destination());
                return new PlayerHandoffCodec.BoundaryResponse(request.playerId(), true, "capture requested");
            });
            final UUID boundaryId = UUID.randomUUID();
            FrameCodec.write(socket.getOutputStream(), frame(
                MessageType.PLAYER_BOUNDARY_REQUEST,
                boundaryId,
                PlayerHandoffCodec.encodeBoundaryRequest(new PlayerHandoffCodec.BoundaryRequest(
                    pushedPlayerId, "backend-id-a", "backend-id-b", destination
                ))
            ));
            final ProtocolFrame boundaryFrame = FrameCodec.read(socket.getInputStream());
            assertEquals(MessageType.PLAYER_BOUNDARY_RESPONSE, boundaryFrame.messageType());
            assertEquals(boundaryId, boundaryFrame.correlationId());
            assertTrue(PlayerHandoffCodec.decodeBoundaryResponse(boundaryFrame.payload()).accepted());

            final UUID settingsId = UUID.randomUUID();
            final var selectedCategories = EnumSet.of(PlayerDataCategory.INVENTORY, PlayerDataCategory.HEALTH);
            FrameCodec.write(socket.getOutputStream(), frame(
                MessageType.PLAYER_SETTINGS_SET,
                settingsId,
                PlayerSettingsCodec.encode(selectedCategories)
            ));
            final ProtocolFrame settings = FrameCodec.read(socket.getInputStream());
            assertEquals(MessageType.PLAYER_SETTINGS_RESPONSE, settings.messageType());
            assertEquals(selectedCategories, PlayerSettingsCodec.decode(settings.payload()));

            final UUID catalogId = UUID.randomUUID();
            FrameCodec.write(socket.getOutputStream(), frame(
                ProtocolChannel.COMMAND,
                MessageType.COMMAND_CATALOG,
                catalogId,
                RemoteCommandCodec.encodeCatalog(new RemoteCommandCodec.Catalog("backend-id-a", Set.of("home")))
            ));
            assertEquals(MessageType.COMMAND_CATALOG_ACK, FrameCodec.read(socket.getInputStream()).messageType());
            assertEquals(Set.of("home"), server.commandCatalog("backend-id-a"));

            final var commandFuture = server.command(
                registry.backendForName("backend-a").orElseThrow(),
                RemoteCommandCodec.Operation.EXECUTE,
                "home spawn"
            );
            final ProtocolFrame commandPush = FrameCodec.read(socket.getInputStream());
            final RemoteCommandCodec.Request commandRequest = RemoteCommandCodec.decodeRequest(commandPush.payload());
            final RemoteCommandCodec.Response commandResponse = new RemoteCommandCodec.Response(
                commandRequest.requestId(), RemoteCommandCodec.Outcome.SUCCESS, "executed", List.of("Done")
            );
            FrameCodec.write(socket.getOutputStream(), frame(
                ProtocolChannel.COMMAND,
                MessageType.COMMAND_RESPONSE,
                UUID.randomUUID(),
                RemoteCommandCodec.encodeResponse(commandResponse)
            ));
            assertEquals(MessageType.COMMAND_CATALOG_ACK, FrameCodec.read(socket.getInputStream()).messageType());
            assertEquals(commandResponse, commandFuture.get(3, TimeUnit.SECONDS));

            final WorldTransactionCodec.Manifest transactionManifest = new WorldTransactionCodec.Manifest(
                UUID.randomUUID(),
                "node-a",
                "node-b",
                "backend-id-a",
                "backend-id-b",
                "minecraft:overworld",
                "world",
                UUID.randomUUID(),
                42L,
                4671,
                WorldTransactionCodec.Axis.X,
                0,
                "node-a",
                "node-b",
                1_024L
            );
            final byte[] transactionKey =
                Base64.getUrlDecoder().decode(configuration.transactionSigningKey());
            final WorldTransactionCodec.Request transactionRequest = new WorldTransactionCodec.Request(
                WorldTransactionCodec.Operation.STATUS,
                WorldTransactionCodec.sign(transactionManifest, transactionKey)
            );
            final var transactionFuture = server.transaction(
                "node-a",
                transactionRequest,
                Duration.ofSeconds(3)
            );
            final ProtocolFrame transactionPush = FrameCodec.read(socket.getInputStream());
            assertEquals(MessageType.WORLD_TRANSACTION_REQUEST, transactionPush.messageType());
            final WorldTransactionCodec.Request decodedTransaction =
                WorldTransactionCodec.decodeRequest(transactionPush.payload());
            assertTrue(WorldTransactionCodec.verify(decodedTransaction.signedManifest(), transactionKey));
            final WorldTransactionCodec.Response transactionResponse = new WorldTransactionCodec.Response(
                transactionManifest.transactionId(),
                WorldTransactionCodec.Operation.STATUS,
                WorldTransactionCodec.Outcome.SUCCESS,
                "node is ready",
                true,
                1234L,
                -1,
                1_000_000L,
                WorldTransactionCodec.digest(transactionManifest)
            );
            FrameCodec.write(socket.getOutputStream(), new ProtocolFrame(
                ShardingbaseProtocol.VERSION,
                ProtocolChannel.WORLD_TRANSACTION,
                MessageType.WORLD_TRANSACTION_RESPONSE,
                transactionPush.correlationId(),
                "node-a",
                "velocity",
                WorldTransactionCodec.encodeResponse(transactionResponse)
            ));
            final WorldTransactionCodec.Response completedTransaction =
                transactionFuture.get(3, TimeUnit.SECONDS);
            assertEquals(transactionResponse.transactionId(), completedTransaction.transactionId());
            assertEquals(transactionResponse.outcome(), completedTransaction.outcome());
            assertArrayEquals(transactionResponse.manifestDigest(), completedTransaction.manifestDigest());

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
        final ProtocolChannel channel,
        final MessageType type,
        final UUID correlationId,
        final byte[] payload
    ) {
        return new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            channel,
            type,
            correlationId,
            "node-a",
            "velocity",
            payload
        );
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
