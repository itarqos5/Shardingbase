package dev.shardingbase.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PlayerSettingsCodecTest {
    @Test
    void roundTripsSelectedCategoriesAndRejectsEmptyMasks() throws Exception {
        final Set<PlayerDataCategory> selected = EnumSet.of(
            PlayerDataCategory.INVENTORY,
            PlayerDataCategory.ADVANCEMENTS
        );
        assertEquals(selected, PlayerSettingsCodec.decode(PlayerSettingsCodec.encode(selected)));
        assertThrows(ProtocolException.class, () -> PlayerSettingsCodec.decode(new byte[Long.BYTES]));
    }
}
