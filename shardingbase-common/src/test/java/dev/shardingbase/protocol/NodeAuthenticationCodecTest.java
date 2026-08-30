package dev.shardingbase.protocol;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeAuthenticationCodecTest {
    @Test
    void roundTripsRequestAndResponse() throws Exception {
        assertEquals("secret", NodeAuthenticationCodec.decodeRequest(NodeAuthenticationCodec.encodeRequest("secret")));
        final NodeAuthenticationCodec.AuthenticationResponse accepted = NodeAuthenticationCodec.decodeResponse(
            NodeAuthenticationCodec.encodeResponse(true, "authenticated")
        );
        assertTrue(accepted.accepted());
        assertEquals("authenticated", accepted.detail());
        assertFalse(NodeAuthenticationCodec.decodeResponse(
            NodeAuthenticationCodec.encodeResponse(false, "rejected")
        ).accepted());
    }

    @Test
    void rejectsTruncatedAndTrailingPayloads() throws Exception {
        final byte[] request = NodeAuthenticationCodec.encodeRequest("secret");
        assertThrows(ProtocolException.class, () -> NodeAuthenticationCodec.decodeRequest(
            Arrays.copyOf(request, request.length - 1)
        ));
        final byte[] response = NodeAuthenticationCodec.encodeResponse(true, "ok");
        final byte[] trailing = Arrays.copyOf(response, response.length + 1);
        assertThrows(ProtocolException.class, () -> NodeAuthenticationCodec.decodeResponse(trailing));
    }
}
