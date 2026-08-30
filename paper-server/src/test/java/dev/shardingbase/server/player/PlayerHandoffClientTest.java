package dev.shardingbase.server.player;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import dev.shardingbase.protocol.PlayerSettingsCodec;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PlayerHandoffClientTest {
    @Test
    void preparesStagesAndFetchesCorrelatedSnapshots() throws Exception {
        final UUID playerId = UUID.randomUUID();
        final AtomicReference<PlayerHandoffCodec.Stage> staged = new AtomicReference<>();
        final EnumSet<PlayerDataCategory> categories = EnumSet.of(PlayerDataCategory.INVENTORY);
        final PlayerHandoffClient client = new PlayerHandoffClient("backend-a", (
            sourceId, channel, messageType, targetId, payload
        ) -> {
            assertEquals("backend-a", sourceId);
            assertEquals("velocity", targetId);
            final UUID correlationId = UUID.randomUUID();
            final byte[] responsePayload;
            final MessageType responseType;
            switch (messageType) {
                case PLAYER_SNAPSHOT_PREPARE -> {
                    final var prepare = PlayerHandoffCodec.decodePrepare(payload);
                    responsePayload = PlayerHandoffCodec.encodeAcknowledgement(
                        new PlayerHandoffCodec.Acknowledgement(prepare.playerId(), 7, true, "reserved")
                    );
                    responseType = MessageType.PLAYER_SNAPSHOT_ACK;
                }
                case PLAYER_SNAPSHOT_STAGE -> {
                    final var stage = PlayerHandoffCodec.decodeStage(payload);
                    staged.set(stage);
                    responsePayload = PlayerHandoffCodec.encodeAcknowledgement(
                        new PlayerHandoffCodec.Acknowledgement(stage.snapshot().playerId(), 7, true, "stored")
                    );
                    responseType = MessageType.PLAYER_SNAPSHOT_ACK;
                }
                case PLAYER_SNAPSHOT_FETCH -> {
                    responsePayload = PlayerHandoffCodec.encodeFetchResponse(
                        new PlayerHandoffCodec.FetchResponse(staged.get())
                    );
                    responseType = MessageType.PLAYER_SNAPSHOT_FETCH_RESPONSE;
                }
                case PLAYER_SETTINGS_GET -> {
                    responsePayload = PlayerSettingsCodec.encode(categories);
                    responseType = MessageType.PLAYER_SETTINGS_RESPONSE;
                }
                case PLAYER_SETTINGS_SET -> {
                    responsePayload = PlayerSettingsCodec.encode(PlayerSettingsCodec.decode(payload));
                    responseType = MessageType.PLAYER_SETTINGS_RESPONSE;
                }
                default -> throw new AssertionError(messageType);
            }
            return new ProtocolFrame(
                ShardingbaseProtocol.VERSION,
                channel,
                responseType,
                correlationId,
                "velocity",
                sourceId,
                responsePayload
            );
        });
        final long revision = client.prepare(playerId, "backend-b", categories);
        final PlayerSnapshot snapshot = new PlayerSnapshot(
            playerId,
            revision,
            "backend-a",
            Map.of(PlayerDataCategory.INVENTORY, new byte[] {1, 2})
        );

        client.stage("backend-b", snapshot);

        assertEquals(7, revision);
        assertTrue(client.fetch(playerId).isPresent());
        assertEquals(7, client.fetch(playerId).orElseThrow().snapshot().revision());
        assertEquals(categories, client.settings());
        assertEquals(categories, client.settings(categories));
    }
}
