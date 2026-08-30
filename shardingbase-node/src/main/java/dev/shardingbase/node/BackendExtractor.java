package dev.shardingbase.node;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.CodeSource;
import java.util.Map;

/**
 * Exports the bundled server without attempting to execute it in-process.
 */
final class BackendExtractor {
    static final String EMBEDDED_BACKEND_PATH = "/META-INF/shardingbase/Shardingbase-server.jar";
    static final String EXPORTED_BACKEND_NAME = "Shardingbase-backend.jar";
    static final String BACKEND_JAR_ENVIRONMENT = "SHARDINGBASE_BACKEND_JAR";

    private BackendExtractor() {
    }

    static ExtractionResult extractEmbeddedBackend() throws IOException, URISyntaxException {
        final InputStream resource = BackendExtractor.class.getResourceAsStream(EMBEDDED_BACKEND_PATH);
        if (resource == null) {
            throw new IOException("Manager JAR does not contain " + EMBEDDED_BACKEND_PATH);
        }

        try (resource) {
            return extract(resource, managerDirectory().resolve(exportedBackendName(System.getenv())));
        }
    }

    static String exportedBackendName(final Map<String, String> environment) throws IOException {
        final String configured = environment.getOrDefault(BACKEND_JAR_ENVIRONMENT, EXPORTED_BACKEND_NAME).trim();
        if (configured.isEmpty()
            || !configured.endsWith(".jar")
            || !Path.of(configured).getFileName().toString().equals(configured)) {
            throw new IOException(BACKEND_JAR_ENVIRONMENT + " must be a plain .jar filename");
        }
        return configured;
    }

    static ExtractionResult extract(final InputStream source, final Path target) throws IOException {
        final Path normalizedTarget = target.toAbsolutePath().normalize();
        final Path parent = normalizedTarget.getParent();
        if (parent == null) {
            throw new IOException("Backend target has no parent directory: " + normalizedTarget);
        }
        Files.createDirectories(parent);

        final Path temporary = Files.createTempFile(parent, ".shardingbase-backend-", ".jar.tmp");
        try {
            Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING);
            if (Files.isRegularFile(normalizedTarget) && Files.mismatch(temporary, normalizedTarget) == -1L) {
                return new ExtractionResult(normalizedTarget, false);
            }

            try {
                Files.move(
                    temporary,
                    normalizedTarget,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
                );
            } catch (AtomicMoveNotSupportedException _) {
                Files.move(temporary, normalizedTarget, StandardCopyOption.REPLACE_EXISTING);
            }
            return new ExtractionResult(normalizedTarget, true);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static Path managerDirectory() throws URISyntaxException {
        final CodeSource codeSource = ShardingbaseNode.class.getProtectionDomain().getCodeSource();
        if (codeSource != null) {
            final URL locationUrl = codeSource.getLocation();
            if ("file".equalsIgnoreCase(locationUrl.getProtocol())) {
                final Path location = Path.of(locationUrl.toURI()).toAbsolutePath().normalize();
                if (Files.isRegularFile(location) && location.getParent() != null) {
                    return location.getParent();
                }
            }
        }
        return Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
    }

    record ExtractionResult(Path path, boolean updated) {
    }
}
