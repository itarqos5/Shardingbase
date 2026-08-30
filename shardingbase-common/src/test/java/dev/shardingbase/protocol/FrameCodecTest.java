package dev.shardingbase.protocol;

import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationRequest;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FrameCodecTest {
    @Test
    void roundTripsAFrame() throws Exception {
        final ProtocolFrame expected = new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            ProtocolChannel.CONTROL,
            MessageType.VALIDATE_BACKEND_REQUEST,
            UUID.fromString("c35b30b0-665f-44ba-956f-180691638c6c"),
            "node-a",
            "velocity",
            new byte[] {1, 2, 3, 4}
        );
        final ByteArrayOutputStream encoded = new ByteArrayOutputStream();

        FrameCodec.write(encoded, expected);

        assertEquals(expected, FrameCodec.read(new ByteArrayInputStream(encoded.toByteArray())));
    }

    @Test
    void rejectsCorruption() throws Exception {
        final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        FrameCodec.write(encoded, frame(new byte[] {1, 2, 3}));
        final byte[] corrupt = encoded.toByteArray();
        corrupt[corrupt.length - 33] ^= 0x01;

        assertThrows(ProtocolException.class, () -> FrameCodec.read(new ByteArrayInputStream(corrupt)));
    }

    @Test
    void rejectsOversizedLengthBeforeAllocating() throws Exception {
        final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(encoded)) {
            output.writeInt(Integer.MAX_VALUE);
        }

        assertThrows(ProtocolException.class, () -> FrameCodec.read(new ByteArrayInputStream(encoded.toByteArray())));
    }

    @Test
    void payloadIsDefensivelyCopied() {
        final byte[] original = {1, 2};
        final ProtocolFrame frame = frame(original);
        original[0] = 9;
        final byte[] exposed = frame.payload();
        exposed[1] = 9;

        assertArrayEquals(new byte[] {1, 2}, frame.payload());
        assertNotEquals(frame.hashCode(), frame(new byte[] {9, 9}).hashCode());
    }

    @Test
    void roundTripsValidationPayloads() throws Exception {
        final ValidationRequest request = new ValidationRequest("secret", "id-a", "backend-a", "26.2", "26.2-test");
        final ValidationResponse response = new ValidationResponse(true, "accepted", "id-b", "backend-b");

        assertEquals(request, ValidationPayloadCodec.decodeRequest(ValidationPayloadCodec.encodeRequest(request)));
        assertEquals(response, ValidationPayloadCodec.decodeResponse(ValidationPayloadCodec.encodeResponse(response)));
    }

    private static ProtocolFrame frame(final byte[] payload) {
        return new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            ProtocolChannel.CONTROL,
            MessageType.HEARTBEAT,
            UUID.fromString("c35b30b0-665f-44ba-956f-180691638c6c"),
            "source",
            "target",
            payload
        );
    }
}
