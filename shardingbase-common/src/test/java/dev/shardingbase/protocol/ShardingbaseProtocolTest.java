package dev.shardingbase.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardingbaseProtocolTest {
    @Test
    void exposesStableInitialProtocolIdentity() {
        assertEquals("shardingbase:sync", ShardingbaseProtocol.PLAYER_SYNC_CHANNEL);
        assertTrue(ShardingbaseProtocol.VERSION > 0);
    }
}
