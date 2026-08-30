package dev.shardingbase.node;

import dev.shardingbase.protocol.FileTransferCodec.Begin;
import dev.shardingbase.protocol.FileTransferCodec.Chunk;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.UUID;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@NullMarked
class ResumableFileReceiverTest {
    @Test
    void resumesAfterReceiverRestartAndAtomicallyPublishes(@TempDir final Path directory) throws Exception {
        final UUID transferId = UUID.randomUUID();
        final byte[] content = new byte[] {1, 2, 3, 4};
        final Begin begin = new Begin(transferId, "world/region/r.0.0.mca", content.length, sha256(content));
        final ResumableFileReceiver first = new ResumableFileReceiver(directory);
        assertEquals(0, first.begin(begin).nextOffset());
        assertEquals(2, first.chunk(new Chunk(transferId, 0, new byte[] {1, 2})).nextOffset());

        final ResumableFileReceiver resumed = new ResumableFileReceiver(directory);
        assertEquals(2, resumed.begin(begin).nextOffset());
        assertEquals(2, resumed.chunk(new Chunk(transferId, 0, new byte[] {1})).nextOffset());
        assertEquals(4, resumed.chunk(new Chunk(transferId, 2, new byte[] {3, 4})).nextOffset());
        assertTrue(resumed.complete(transferId).complete());
        assertArrayEquals(content, Files.readAllBytes(directory.resolve("world/region/r.0.0.mca")));
    }

    @Test
    void rejectsTraversalAndWrongCompletedHash(@TempDir final Path directory) throws Exception {
        final ResumableFileReceiver receiver = new ResumableFileReceiver(directory);
        assertThrows(IOException.class, () -> receiver.begin(new Begin(
            UUID.randomUUID(), "../escape", 0, sha256(new byte[0])
        )));

        final UUID transferId = UUID.randomUUID();
        receiver.begin(new Begin(transferId, "file.bin", 1, sha256(new byte[] {9})));
        receiver.chunk(new Chunk(transferId, 0, new byte[] {1}));
        assertThrows(IOException.class, () -> receiver.complete(transferId));
        assertFalse(Files.exists(directory.resolve("file.bin")));
    }

    private static byte[] sha256(final byte[] content) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(content);
    }
}
