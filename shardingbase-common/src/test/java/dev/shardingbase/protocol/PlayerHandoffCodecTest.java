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

        final PlayerHandoffCodec.TransferDestination destination = new PlayerHandoffCodec.TransferDestination(
            "minecraft:overworld", UUID.randomUUID(), 25_000.25, 70.0, -16.25, 90.0F, -12.0F
        );
        final PlayerHandoffCodec.Capture capture = PlayerHandoffCodec.decodeCapture(
            PlayerHandoffCodec.encodeCapture(new PlayerHandoffCodec.Capture(
                playerId,
                "backend-b",
                41,
                EnumSet.of(PlayerDataCategory.INVENTORY, PlayerDataCategory.EXPERIENCE),
                destination
            ))
        );
        assertEquals(41, capture.revision());
        assertTrue(capture.categories().contains(PlayerDataCategory.INVENTORY));
        assertEquals(destination, capture.destination());

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
            ), destination)
        ));
        assertEquals("backend-b", stage.targetBackendId());
        assertEquals(42, stage.snapshot().revision());
        assertEquals(destination, stage.destination());

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

        final PlayerHandoffCodec.BoundaryRequest boundary = PlayerHandoffCodec.decodeBoundaryRequest(
            PlayerHandoffCodec.encodeBoundaryRequest(new PlayerHandoffCodec.BoundaryRequest(
                playerId, "backend-a", "backend-b", destination
            ))
        );
        assertEquals(destination, boundary.destination());
        assertEquals(25_000.25, boundary.destination().x());
        assertEquals("backend-a", boundary.sourceBackendId());

        final PlayerHandoffCodec.BoundaryResponse boundaryResponse = PlayerHandoffCodec.decodeBoundaryResponse(
            PlayerHandoffCodec.encodeBoundaryResponse(new PlayerHandoffCodec.BoundaryResponse(
                playerId, true, "capture requested"
            ))
        );
        assertTrue(boundaryResponse.accepted());
    }

    @Test
    void rejectsEmptyCategorySelection() {
        assertThrows(IllegalArgumentException.class, () -> new PlayerHandoffCodec.Prepare(
            UUID.randomUUID(), "backend-b", EnumSet.noneOf(PlayerDataCategory.class)
        ));
    }
}
