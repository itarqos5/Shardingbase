package dev.shardingbase.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RemoteOperationCodecTest {
    @Test
    void roundTripsRequestsAndResponses() throws Exception {
        final UUID id = UUID.randomUUID();
        final RemoteOperationCodec.Request request = new RemoteOperationCodec.Request(
            id, "backend-a", RemoteOperationCodec.Operation.SET_BLOCK_DATA,
            "minecraft:overworld", -3, 64, 5, "minecraft:stone", Map.of("physics", "false")
        );
        assertEquals(request, RemoteOperationCodec.decodeRequest(RemoteOperationCodec.encodeRequest(request)));
        final RemoteOperationCodec.Response response = new RemoteOperationCodec.Response(
            id, RemoteOperationCodec.Outcome.SUCCESS, "set", "", Map.of()
        );
        assertEquals(response, RemoteOperationCodec.decodeResponse(RemoteOperationCodec.encodeResponse(response)));
    }
}
