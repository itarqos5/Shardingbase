package dev.shardingbase.node;

import dev.shardingbase.protocol.FileTransferCodec.Ack;
import dev.shardingbase.protocol.FileTransferCodec.Begin;
import dev.shardingbase.protocol.FileTransferCodec.Chunk;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Disk-backed receiver which resumes only matching, hash-verified transfers. */
final class ResumableFileReceiver {
    private static final Set<String> METADATA_FIELDS = Set.of("relative-path", "total-bytes", "sha256");

    private final Path stagingRoot;
    private final Path transferStateRoot;

    ResumableFileReceiver(final Path stagingRoot) throws IOException {
        this.stagingRoot = stagingRoot.toAbsolutePath().normalize();
        this.transferStateRoot = this.stagingRoot.resolve(".transfers");
        Files.createDirectories(this.transferStateRoot);
    }

    synchronized Ack begin(final Begin begin) throws IOException {
        final TransferPaths paths = this.paths(begin.transferId(), begin.relativePath());
        final String expectedHash = HexFormat.of().formatHex(begin.sha256());
        if (Files.exists(paths.metadata())) {
            final TransferMetadata metadata = this.loadMetadata(paths.metadata());
            if (!metadata.relativePath().equals(begin.relativePath())
                || metadata.totalBytes() != begin.totalBytes()
                || !metadata.sha256().equals(expectedHash)) {
                throw new IOException("Transfer ID is already bound to a different file manifest");
            }
        } else {
            this.writeMetadata(paths.metadata(), new TransferMetadata(
                begin.relativePath(), begin.totalBytes(), expectedHash
            ));
        }
        if (Files.notExists(paths.partial())) {
            Files.createFile(paths.partial());
        }
        final long offset = Files.size(paths.partial());
        if (offset > begin.totalBytes()) {
            throw new IOException("Partial transfer exceeds the declared file size");
        }
        return new Ack(begin.transferId(), offset, false, offset == 0 ? "ready" : "resuming");
    }

    synchronized Ack chunk(final Chunk chunk) throws IOException {
        final Path metadataPath = this.metadataPath(chunk.transferId());
        final TransferMetadata metadata = this.loadMetadata(metadataPath);
        final TransferPaths paths = this.paths(chunk.transferId(), metadata.relativePath());
        if (Files.notExists(paths.partial())) {
            throw new IOException("Transfer partial file is missing");
        }
        final long offset = Files.size(paths.partial());
        if (chunk.offset() != offset) {
            return new Ack(chunk.transferId(), offset, false, "resume at acknowledged offset");
        }
        if (chunk.data().length > metadata.totalBytes() - offset) {
            throw new IOException("File chunk exceeds the declared transfer size");
        }
        try (FileChannel channel = FileChannel.open(paths.partial(), StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            final ByteBuffer data = ByteBuffer.wrap(chunk.data());
            while (data.hasRemaining()) {
                channel.write(data);
            }
            channel.force(false);
        }
        return new Ack(chunk.transferId(), Files.size(paths.partial()), false, "chunk stored");
    }

    synchronized Ack complete(final UUID transferId) throws IOException {
        final Path metadataPath = this.metadataPath(transferId);
        final TransferMetadata metadata = this.loadMetadata(metadataPath);
        final TransferPaths paths = this.paths(transferId, metadata.relativePath());
        final long size = Files.size(paths.partial());
        if (size != metadata.totalBytes()) {
            return new Ack(transferId, size, false, "file is incomplete");
        }
        if (!MessageDigest.isEqual(
            HexFormat.of().parseHex(metadata.sha256()),
            sha256(paths.partial())
        )) {
            throw new IOException("Completed file SHA-256 does not match its manifest");
        }
        final Path parent = paths.target().getParent();
        if (parent == null) {
            throw new IOException("Transfer target has no parent directory");
        }
        Files.createDirectories(parent);
        try {
            Files.move(paths.partial(), paths.target(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (final AtomicMoveNotSupportedException exception) {
            throw new IOException("Atomic transfer publication is not supported", exception);
        }
        Files.delete(metadataPath);
        return new Ack(transferId, size, true, "complete");
    }

    synchronized void abort(final UUID transferId) throws IOException {
        Files.deleteIfExists(this.partialPath(transferId));
        Files.deleteIfExists(this.metadataPath(transferId));
    }

    private TransferPaths paths(final UUID transferId, final String relativePath) throws IOException {
        final Path relative;
        try {
            relative = Path.of(relativePath).normalize();
        } catch (final RuntimeException exception) {
            throw new IOException("Invalid transfer path", exception);
        }
        if (relative.isAbsolute() || relative.getNameCount() == 0 || relative.startsWith("..")) {
            throw new IOException("Transfer path must remain relative to the staging root");
        }
        final Path target = this.stagingRoot.resolve(relative).normalize();
        if (!target.startsWith(this.stagingRoot) || target.startsWith(this.transferStateRoot)) {
            throw new IOException("Transfer path escapes the staging root");
        }
        return new TransferPaths(target, this.partialPath(transferId), this.metadataPath(transferId));
    }

    private Path partialPath(final UUID transferId) {
        return this.transferStateRoot.resolve(transferId + ".part");
    }

    private Path metadataPath(final UUID transferId) {
        return this.transferStateRoot.resolve(transferId + ".properties");
    }

    private TransferMetadata loadMetadata(final Path path) throws IOException {
        if (!path.startsWith(this.transferStateRoot) || Files.notExists(path)) {
            throw new IOException("Unknown file transfer");
        }
        final Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
        if (!properties.stringPropertyNames().equals(METADATA_FIELDS)) {
            throw new IOException("File transfer metadata has missing or unknown fields");
        }
        try {
            final String hash = properties.getProperty("sha256");
            if (hash == null || !hash.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("invalid SHA-256");
            }
            return new TransferMetadata(
                properties.getProperty("relative-path"),
                Long.parseLong(properties.getProperty("total-bytes")),
                hash
            );
        } catch (final IllegalArgumentException | NullPointerException exception) {
            throw new IOException("Invalid file transfer metadata", exception);
        }
    }

    private void writeMetadata(final Path target, final TransferMetadata metadata) throws IOException {
        final Properties properties = new Properties();
        properties.setProperty("relative-path", metadata.relativePath());
        properties.setProperty("total-bytes", Long.toString(metadata.totalBytes()));
        properties.setProperty("sha256", metadata.sha256());
        final Path temporary = Files.createTempFile(this.transferStateRoot, ".metadata-", ".tmp");
        try {
            try (OutputStream output = Files.newOutputStream(temporary)) {
                properties.store(output, "Shardingbase resumable file transfer");
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (final AtomicMoveNotSupportedException exception) {
                throw new IOException("Atomic transfer metadata publication is not supported", exception);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static byte[] sha256(final Path file) throws IOException {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(file)) {
                final byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, read);
                }
            }
            return digest.digest();
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required", exception);
        }
    }

    private record TransferPaths(Path target, Path partial, Path metadata) {
    }

    private record TransferMetadata(String relativePath, long totalBytes, String sha256) {
        private TransferMetadata {
            if (relativePath == null || relativePath.isBlank() || totalBytes < 0 || sha256 == null) {
                throw new IllegalArgumentException("Invalid transfer metadata");
            }
        }
    }
}
