package dev.shardingbase.protocol;

import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlayerHandoffCodecTest {
    @Test
    void roundTripsPreparationAndAcknowledgement() throws Exception {
        final UUID playerId = UUID.randomUUID();
        final PlayerHandoffCodec.Prepare prepare = PlayerHandoffCodec.decodePrepare(PlayerHandoffCodec.encodePrepare(
            new PlayerHandoffCodec.Prepare(
                playerId,
                "backend-b",
                EnumSet.of(PlayerDataCategory.INVENTORY, PlayerDataCategory.EXPERIENCE)
            )
        ));
        assertEquals(playerId, prepare.playerId());
        assertTrue(prepare.categories().contains(PlayerDataCategory.EXPERIENCE));

        final PlayerHandoffCodec.Acknowledgement acknowledgement = PlayerHandoffCodec.decodeAcknowledgement(
            PlayerHandoffCodec.encodeAcknowledgement(new PlayerHandoffCodec.Acknowledgement(
                playerId, 42, true, "staged"
            ))
        );
        assertEquals(42, acknowledgement.revision());
        assertTrue(acknowledgement.accepted());

        final PlayerHandoffCodec.Stage stage = PlayerHandoffCodec.decodeStage(PlayerHandoffCodec.encodeStage(
            new PlayerHandoffCodec.Stage("backend-b", new PlayerSnapshot(
                playerId,
                42,
                "backend-a",
                Map.of(PlayerDataCategory.INVENTORY, new byte[] {1})
            ))
        ));
        assertEquals("backend-b", stage.targetBackendId());
        assertEquals(42, stage.snapshot().revision());

        final PlayerHandoffCodec.Fetch fetch = PlayerHandoffCodec.decodeFetch(PlayerHandoffCodec.encodeFetch(
            new PlayerHandoffCodec.Fetch(playerId, "backend-b")
        ));
        assertEquals(playerId, fetch.playerId());
        assertEquals("backend-b", fetch.targetBackendId());

        final PlayerHandoffCodec.FetchResponse fetched = PlayerHandoffCodec.decodeFetchResponse(
            PlayerHandoffCodec.encodeFetchResponse(new PlayerHandoffCodec.FetchResponse(stage))
        );
        assertEquals(42, fetched.stage().snapshot().revision());
        final PlayerHandoffCodec.FetchResponse absent = PlayerHandoffCodec.decodeFetchResponse(
            PlayerHandoffCodec.encodeFetchResponse(new PlayerHandoffCodec.FetchResponse(null))
        );
        assertEquals(null, absent.stage());
    }

    @Test
    void rejectsEmptyCategorySelection() {
        assertThrows(IllegalArgumentException.class, () -> new PlayerHandoffCodec.Prepare(
            UUID.randomUUID(), "backend-b", EnumSet.noneOf(PlayerDataCategory.class)
        ));
    }
}
