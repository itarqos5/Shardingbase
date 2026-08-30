package dev.shardingbase.server.player;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/** Durable per-player target ledger preventing duplicate or stale snapshot application. */
final class AppliedPlayerRevisionStore {
    private final Path root;

    AppliedPlayerRevisionStore(final Path serverDirectory) {
        this.root = serverDirectory.resolve("shardingbase").resolve("player-revisions").toAbsolutePath().normalize();
    }

    synchronized boolean shouldApply(final UUID playerId, final long revision) throws IOException {
        if (revision < 1) {
            return false;
        }
        final Path path = this.path(playerId);
        if (!Files.isRegularFile(path)) {
            return true;
        }
        final String value = Files.readString(path, StandardCharsets.US_ASCII).trim();
        try {
            return revision > Long.parseLong(value);
        } catch (final NumberFormatException exception) {
            throw new IOException("Invalid applied player revision ledger for " + playerId, exception);
        }
    }

    synchronized void markApplied(final UUID playerId, final long revision) throws IOException {
        if (revision < 1) {
            throw new IOException("Applied player revision must be positive");
        }
        Files.createDirectories(this.root);
        final Path target = this.path(playerId);
        final Path temporary = Files.createTempFile(this.root, playerId + "-", ".tmp");
        try {
            Files.writeString(temporary, Long.toString(revision), StandardCharsets.US_ASCII);
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (final java.nio.file.AtomicMoveNotSupportedException _) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private Path path(final UUID playerId) throws IOException {
        final Path path = this.root.resolve(playerId + ".revision").normalize();
        if (!path.getParent().equals(this.root)) {
            throw new IOException("Player revision path escapes its configured root");
        }
        return path;
    }
}
