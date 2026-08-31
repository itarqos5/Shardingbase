package dev.shardingbase.protocol;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorldTransactionCodecTest {
    private static final byte[] KEY =
        "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);

    @Test
    void signsAndRoundTripsOneImmutableManifest() throws Exception {
        final WorldTransactionCodec.Manifest manifest = manifest();
        final WorldTransactionCodec.SignedManifest signed = WorldTransactionCodec.sign(manifest, KEY);
        final WorldTransactionCodec.Request request = new WorldTransactionCodec.Request(
            WorldTransactionCodec.Operation.AUTHORIZE_AND_SAVE,
            signed
        );

        final WorldTransactionCodec.Request decoded =
            WorldTransactionCodec.decodeRequest(WorldTransactionCodec.encodeRequest(request));

        assertEquals(request.operation(), decoded.operation());
        assertEquals(manifest, decoded.signedManifest().manifest());
        assertArrayEquals(signed.signature(), decoded.signedManifest().signature());
        assertTrue(WorldTransactionCodec.verify(decoded.signedManifest(), KEY));
    }

    @Test
    void rejectsTamperingAndUnsafeWorldPaths() throws Exception {
        final WorldTransactionCodec.SignedManifest signed = WorldTransactionCodec.sign(manifest(), KEY);
        final byte[] changed = signed.signature();
        changed[0] ^= 1;

        assertFalse(WorldTransactionCodec.verify(
            new WorldTransactionCodec.SignedManifest(signed.manifest(), changed),
            KEY
        ));
        assertThrows(IllegalArgumentException.class, () -> new WorldTransactionCodec.Manifest(
            UUID.randomUUID(), "node-a", "node-b", "backend-a", "backend-b",
            "minecraft:overworld", "../world", UUID.randomUUID(), 42L, 4671,
            WorldTransactionCodec.Axis.X, 0, "node-a", "node-b", 1L
        ));
    }

    @Test
    void roundTripsBoundedLifecycleResponse() throws Exception {
        final byte[] digest = WorldTransactionCodec.digest(manifest());
        final WorldTransactionCodec.Response response = new WorldTransactionCodec.Response(
            manifest().transactionId(),
            WorldTransactionCodec.Operation.STATUS,
            WorldTransactionCodec.Outcome.SUCCESS,
            "backend is online",
            true,
            1234L,
            -1,
            8_000_000L,
            digest
        );

        final WorldTransactionCodec.Response decoded =
            WorldTransactionCodec.decodeResponse(WorldTransactionCodec.encodeResponse(response));

        assertEquals(response.transactionId(), decoded.transactionId());
        assertEquals(response.operation(), decoded.operation());
        assertEquals(response.outcome(), decoded.outcome());
        assertEquals(response.detail(), decoded.detail());
        assertEquals(response.backendRunning(), decoded.backendRunning());
        assertEquals(response.processId(), decoded.processId());
        assertEquals(response.lastExitCode(), decoded.lastExitCode());
        assertEquals(response.usableBytes(), decoded.usableBytes());
        assertArrayEquals(response.manifestDigest(), decoded.manifestDigest());
    }

    @Test
    void rejectsTrailingPayloadData() throws Exception {
        final byte[] encoded = WorldTransactionCodec.encodeRequest(new WorldTransactionCodec.Request(
            WorldTransactionCodec.Operation.STATUS,
            WorldTransactionCodec.sign(manifest(), KEY)
        ));
        assertThrows(IOException.class, () -> WorldTransactionCodec.decodeRequest(
            Arrays.copyOf(encoded, encoded.length + 1)
        ));
    }

    private static WorldTransactionCodec.Manifest manifest() {
        return new WorldTransactionCodec.Manifest(
            UUID.fromString("4cb40e58-d4ca-44c0-a9ce-678f926b654a"),
            "node-a",
            "node-b",
            "backend-a",
            "backend-b",
            "minecraft:overworld",
            "world",
            UUID.fromString("ddc72ef4-9fc1-4df4-b529-993e0e85a126"),
            42L,
            4671,
            WorldTransactionCodec.Axis.X,
            -2,
            "node-a",
            "node-b",
            64_000_000L
        );
    }
}
