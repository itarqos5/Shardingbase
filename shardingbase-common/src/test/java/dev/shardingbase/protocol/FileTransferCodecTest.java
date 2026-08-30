package dev.shardingbase.protocol;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileTransferCodecTest {
    @Test
    void roundTripsBeginChunkAndAcknowledgement() throws Exception {
        final UUID transferId = UUID.randomUUID();
        final byte[] data = new byte[] {1, 2, 3};
        final FileTransferCodec.Begin begin = FileTransferCodec.decodeBegin(FileTransferCodec.encodeBegin(
            new FileTransferCodec.Begin(transferId, "world/region/r.0.0.mca", data.length, sha256(data))
        ));
        assertEquals(transferId, begin.transferId());
        assertEquals("world/region/r.0.0.mca", begin.relativePath());
        final FileTransferCodec.Chunk chunk = FileTransferCodec.decodeChunk(FileTransferCodec.encodeChunk(
            new FileTransferCodec.Chunk(transferId, 0, data)
        ));
        assertArrayEquals(data, chunk.data());
        final FileTransferCodec.Ack ack = FileTransferCodec.decodeAck(FileTransferCodec.encodeAck(
            new FileTransferCodec.Ack(transferId, 3, true, "complete")
        ));
        assertEquals(3, ack.nextOffset());
    }

    @Test
    void rejectsCorruptedChunk() throws Exception {
        final byte[] encoded = FileTransferCodec.encodeChunk(new FileTransferCodec.Chunk(
            UUID.randomUUID(), 0, new byte[] {1, 2, 3}
        ));
        encoded[encoded.length - 1] ^= 1;
        assertThrows(ProtocolException.class, () -> FileTransferCodec.decodeChunk(encoded));
    }

    @Test
    void rejectsOversizedChunk() {
        assertThrows(IllegalArgumentException.class, () -> new FileTransferCodec.Chunk(
            UUID.randomUUID(),
            0,
            new byte[FileTransferCodec.MAX_CHUNK_BYTES + 1]
        ));
    }

    private static byte[] sha256(final byte[] data) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(data, data.length));
    }
}
