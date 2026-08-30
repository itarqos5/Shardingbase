package dev.shardingbase.node.world;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Atomically persisted transaction state used to decide resume versus rollback. */
public final class WorldTransactionJournal {
    private static final Map<TransactionPhase, Set<TransactionPhase>> TRANSITIONS = transitions();

    private final Path path;
    private final UUID transactionId;
    private TransactionPhase phase;

    private WorldTransactionJournal(final Path path, final UUID transactionId, final TransactionPhase phase) {
        this.path = path;
        this.transactionId = transactionId;
        this.phase = phase;
    }

    public static WorldTransactionJournal create(final Path path, final UUID transactionId) throws IOException {
        final Path normalized = path.toAbsolutePath().normalize();
        if (Files.exists(normalized)) {
            throw new IOException("Transaction journal already exists: " + normalized);
        }
        final WorldTransactionJournal journal = new WorldTransactionJournal(normalized, transactionId, TransactionPhase.PLANNED);
        journal.persist();
        return journal;
    }

    public static WorldTransactionJournal load(final Path path) throws IOException {
        final Path normalized = path.toAbsolutePath().normalize();
        final Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(normalized)) {
            properties.load(input);
        }
        if (!properties.stringPropertyNames().equals(Set.of("transaction-id", "phase", "updated"))) {
            throw new IOException("Transaction journal has missing or unknown fields: " + normalized);
        }
        try {
            return new WorldTransactionJournal(
                normalized,
                UUID.fromString(properties.getProperty("transaction-id")),
                TransactionPhase.valueOf(properties.getProperty("phase"))
            );
        } catch (final IllegalArgumentException exception) {
            throw new IOException("Transaction journal is invalid: " + normalized, exception);
        }
    }

    public synchronized void advance(final TransactionPhase next) throws IOException {
        if (!TRANSITIONS.getOrDefault(this.phase, Set.of()).contains(next)) {
            throw new IOException("Illegal transaction transition " + this.phase + " -> " + next);
        }
        final TransactionPhase previous = this.phase;
        this.phase = next;
        try {
            this.persist();
        } catch (final IOException exception) {
            this.phase = previous;
            throw exception;
        }
    }

    public UUID transactionId() {
        return this.transactionId;
    }

    public synchronized TransactionPhase phase() {
        return this.phase;
    }

    private void persist() throws IOException {
        final Path parent = this.path.getParent();
        if (parent == null) {
            throw new IOException("Transaction journal has no parent directory");
        }
        Files.createDirectories(parent);
        final Properties properties = new Properties();
        properties.setProperty("transaction-id", this.transactionId.toString());
        properties.setProperty("phase", this.phase.name());
        properties.setProperty("updated", Instant.now().toString());
        final Path temporary = Files.createTempFile(parent, ".shardingbase-journal-", ".tmp");
        try {
            try (OutputStream output = Files.newOutputStream(
                temporary,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )) {
                properties.store(output, "Shardingbase world transaction journal");
            }
            try {
                Files.move(temporary, this.path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (final AtomicMoveNotSupportedException exception) {
                throw new IOException("Atomic transaction journal replacement is not supported", exception);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static Map<TransactionPhase, Set<TransactionPhase>> transitions() {
        final EnumMap<TransactionPhase, Set<TransactionPhase>> transitions = new EnumMap<>(TransactionPhase.class);
        transitions.put(TransactionPhase.PLANNED, EnumSet.of(TransactionPhase.AUTHORIZED, TransactionPhase.FAILED));
        transitions.put(TransactionPhase.AUTHORIZED, EnumSet.of(TransactionPhase.BACKEND_STOPPED, TransactionPhase.FAILED));
        transitions.put(TransactionPhase.BACKEND_STOPPED, EnumSet.of(TransactionPhase.BACKUP_COMPLETE, TransactionPhase.FAILED));
        transitions.put(TransactionPhase.BACKUP_COMPLETE, EnumSet.of(TransactionPhase.SPLIT_COMPLETE, TransactionPhase.ROLLED_BACK));
        transitions.put(TransactionPhase.SPLIT_COMPLETE, EnumSet.of(TransactionPhase.TARGET_PREPARED, TransactionPhase.ROLLED_BACK));
        transitions.put(TransactionPhase.TARGET_PREPARED, EnumSet.of(TransactionPhase.SOURCE_COMMITTED, TransactionPhase.ROLLED_BACK));
        transitions.put(TransactionPhase.SOURCE_COMMITTED, EnumSet.of(TransactionPhase.STARTING_TARGET, TransactionPhase.ROLLED_BACK));
        transitions.put(TransactionPhase.STARTING_TARGET, EnumSet.of(TransactionPhase.STARTING_SOURCE, TransactionPhase.ROLLED_BACK));
        transitions.put(TransactionPhase.STARTING_SOURCE, EnumSet.of(TransactionPhase.COMPLETE, TransactionPhase.ROLLED_BACK));
        return Map.copyOf(transitions);
    }
}
