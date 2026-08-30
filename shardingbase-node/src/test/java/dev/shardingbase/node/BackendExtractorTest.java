package dev.shardingbase.node;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackendExtractorTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void exportsAndReplacesAnOutdatedBackend() throws Exception {
        final Path target = this.temporaryDirectory.resolve(BackendExtractor.EXPORTED_BACKEND_NAME);
        Files.writeString(target, "outdated", StandardCharsets.UTF_8);
        final byte[] backend = "new backend".getBytes(StandardCharsets.UTF_8);

        final BackendExtractor.ExtractionResult result = BackendExtractor.extract(
            new ByteArrayInputStream(backend),
            target
        );

        assertTrue(result.updated());
        assertArrayEquals(backend, Files.readAllBytes(target));
    }

    @Test
    void keepsAnIdenticalBackend() throws Exception {
        final Path target = this.temporaryDirectory.resolve(BackendExtractor.EXPORTED_BACKEND_NAME);
        final byte[] backend = "same backend".getBytes(StandardCharsets.UTF_8);
        Files.write(target, backend);

        final BackendExtractor.ExtractionResult result = BackendExtractor.extract(
            new ByteArrayInputStream(backend),
            target
        );

        assertFalse(result.updated());
        assertArrayEquals(backend, Files.readAllBytes(target));
    }

    @Test
    void validatesConfiguredBackendFilename() throws Exception {
        assertEquals("Shardingbase-backend.jar", BackendExtractor.exportedBackendName(Map.of()));
        assertEquals("custom.jar", BackendExtractor.exportedBackendName(Map.of(
            BackendExtractor.BACKEND_JAR_ENVIRONMENT, "custom.jar"
        )));
        assertThrows(IOException.class, () -> BackendExtractor.exportedBackendName(Map.of(
            BackendExtractor.BACKEND_JAR_ENVIRONMENT, "../escape.jar"
        )));
    }
}
