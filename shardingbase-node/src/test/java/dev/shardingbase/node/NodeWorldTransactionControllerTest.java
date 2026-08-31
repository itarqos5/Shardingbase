package dev.shardingbase.node;

import java.io.IOException;
import java.util.Base64;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeWorldTransactionControllerTest {
    @Test
    void requiresAFullStrengthUrlSafeTransactionKey() throws Exception {
        final byte[] key = new byte[32];
        for (int index = 0; index < key.length; index++) {
            key[index] = (byte) index;
        }

        assertArrayEquals(
            key,
            NodeWorldTransactionController.signingKey(
                Base64.getUrlEncoder().withoutPadding().encodeToString(key)
            )
        );
        assertThrows(IOException.class, () ->
            NodeWorldTransactionController.signingKey(Base64.getUrlEncoder().encodeToString(new byte[16])));
        assertThrows(IOException.class, () -> NodeWorldTransactionController.signingKey("not base64!"));
    }

    @Test
    void reservesTwentyPercentWithoutOverflow() {
        assertEquals(120L, NodeWorldTransactionController.safetyMargin(100L));
        assertEquals(Long.MAX_VALUE, NodeWorldTransactionController.safetyMargin(Long.MAX_VALUE));
    }
}
