package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransferTreeManifestTest {
    @Test
    void verifiesTheExactRelayedFileSetAndEveryHash(@TempDir final Path directory) throws Exception {
        Files.createDirectories(directory.resolve("region"));
        Files.write(directory.resolve("region/r.0.0.mca"), new byte[] {1, 2, 3});
        Files.writeString(directory.resolve("level.dat"), "metadata");

        final TransferTreeManifest.Summary written = TransferTreeManifest.write(directory);
        final TransferTreeManifest.Summary verified = TransferTreeManifest.verify(directory);

        assertEquals(2, written.files());
        assertEquals(written, verified);
        assertEquals(3, TransferTreeManifest.filesForTransfer(directory).size());
    }

    @Test
    void rejectsTamperingAndUnmanifestedFiles(@TempDir final Path directory) throws Exception {
        Files.writeString(directory.resolve("level.dat"), "original");
        TransferTreeManifest.write(directory);
        Files.writeString(directory.resolve("level.dat"), "changed");
        assertThrows(IOException.class, () -> TransferTreeManifest.verify(directory));

        Files.writeString(directory.resolve("level.dat"), "original");
        TransferTreeManifest.write(directory);
        Files.writeString(directory.resolve("unexpected.dat"), "extra");
        assertThrows(IOException.class, () -> TransferTreeManifest.verify(directory));
    }
}
