package dev.shardingbase.server.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LocalNodeClientTest {
    @Test
    void authenticatesAndCorrelatesFramedRequests() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            final CompletableFuture<Void> handled = CompletableFuture.runAsync(() -> {
                try (var socket = server.accept();
                     var input = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
                    assertEquals(ShardingbaseProtocol.MAGIC, input.readInt());
                    assertEquals(ShardingbaseProtocol.VERSION, input.readInt());
                    assertEquals("secret", readField(input));
                    final ProtocolFrame request = FrameCodec.read(input);
                    FrameCodec.write(socket.getOutputStream(), new ProtocolFrame(
                        ShardingbaseProtocol.VERSION,
                        request.channel(),
                        MessageType.HEARTBEAT_ACK,
                        request.correlationId(),
                        "node",
                        request.sourceId(),
                        new byte[0]
                    ));
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            });
            final LocalNodeClient client = new LocalNodeClient(Map.of(
                LocalNodeClient.PORT_ENVIRONMENT_VARIABLE, Integer.toString(server.getLocalPort()),
                LocalNodeClient.TOKEN_ENVIRONMENT_VARIABLE, "secret"
            ));

            final ProtocolFrame response = client.request(
                "backend-a",
                ProtocolChannel.CONTROL,
                MessageType.HEARTBEAT,
                "velocity",
                new byte[0]
            );

            assertEquals(MessageType.HEARTBEAT_ACK, response.messageType());
            handled.get(5, TimeUnit.SECONDS);
        }
    }

    private static String readField(final DataInputStream input) throws Exception {
        return new String(input.readNBytes(input.readInt()), StandardCharsets.UTF_8);
    }
}
