package dev.shardingbase.velocity;

import dev.shardingbase.protocol.FrameCodec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerDataCategory;
import java.util.EnumSet;
import java.util.Set;

/** SQLite authority for monotonic, idempotent portable player snapshots. */
final class PlayerStateStore {
    private final String jdbcUrl;

    PlayerStateStore(final Path databasePath) throws IOException {
        Files.createDirectories(databasePath.toAbsolutePath().normalize().getParent());
        this.jdbcUrl = "jdbc:sqlite:" + databasePath.toAbsolutePath().normalize();
        try (Connection connection = this.connection()) {
            try (PreparedStatement statement = connection.prepareStatement("""
                CREATE TABLE IF NOT EXISTS player_snapshots (
                    player_uuid TEXT PRIMARY KEY NOT NULL,
                    revision INTEGER NOT NULL CHECK (revision > 0),
                    source_backend_id TEXT NOT NULL,
                    snapshot BLOB NOT NULL,
                    updated_epoch_ms INTEGER NOT NULL
                )
                """)) {
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement("""
                CREATE TABLE IF NOT EXISTS player_revisions (
                    player_uuid TEXT PRIMARY KEY NOT NULL,
                    reserved_revision INTEGER NOT NULL CHECK (reserved_revision > 0)
                )
                """)) {
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement("""
                CREATE TABLE IF NOT EXISTS player_sync_settings (
                    singleton_id INTEGER PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
                    category_mask INTEGER NOT NULL
                )
                """)) {
                statement.executeUpdate();
            }
        } catch (final SQLException exception) {
            throw failure("Unable to initialize player snapshot storage", exception);
        }
    }

    synchronized Set<PlayerDataCategory> categories() throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT category_mask FROM player_sync_settings WHERE singleton_id = 1"
        ); ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                return Set.copyOf(EnumSet.allOf(PlayerDataCategory.class));
            }
            return categoriesFromMask(result.getLong("category_mask"));
        } catch (final SQLException exception) {
            throw failure("Unable to load player synchronization settings", exception);
        }
    }

    synchronized void categories(final Set<PlayerDataCategory> categories) throws IOException {
        if (categories == null || categories.isEmpty()) {
            throw new IOException("At least one portable player category must remain enabled");
        }
        long bits = 0;
        for (final PlayerDataCategory category : categories) {
            bits |= 1L << category.ordinal();
        }
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO player_sync_settings (singleton_id, category_mask) VALUES (1, ?)
            ON CONFLICT(singleton_id) DO UPDATE SET category_mask = excluded.category_mask
            """)) {
            statement.setLong(1, bits);
            statement.executeUpdate();
        } catch (final SQLException exception) {
            throw failure("Unable to store player synchronization settings", exception);
        }
    }

    private static Set<PlayerDataCategory> categoriesFromMask(final long bits) throws IOException {
        long knownBits = 0;
        final EnumSet<PlayerDataCategory> categories = EnumSet.noneOf(PlayerDataCategory.class);
        for (final PlayerDataCategory category : PlayerDataCategory.values()) {
            final long bit = 1L << category.ordinal();
            knownBits |= bit;
            if ((bits & bit) != 0) {
                categories.add(category);
            }
        }
        if (categories.isEmpty() || (bits & ~knownBits) != 0) {
            throw new IOException("Stored player synchronization category mask is invalid");
        }
        return Set.copyOf(categories);
    }

    synchronized long reserveRevision(final UUID playerId) throws IOException {
        try (Connection connection = this.connection()) {
            connection.setAutoCommit(false);
            try {
                long current = 0;
                try (PreparedStatement statement = connection.prepareStatement("""
                    SELECT MAX(revision) AS current_revision FROM (
                        SELECT revision FROM player_snapshots WHERE player_uuid = ?
                        UNION ALL
                        SELECT reserved_revision AS revision FROM player_revisions WHERE player_uuid = ?
                    )
                    """)) {
                    statement.setString(1, playerId.toString());
                    statement.setString(2, playerId.toString());
                    try (ResultSet result = statement.executeQuery()) {
                        if (result.next()) {
                            current = result.getLong("current_revision");
                        }
                    }
                }
                final long next = Math.addExact(current, 1);
                try (PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO player_revisions (player_uuid, reserved_revision) VALUES (?, ?)
                    ON CONFLICT(player_uuid) DO UPDATE SET reserved_revision = excluded.reserved_revision
                    """)) {
                    statement.setString(1, playerId.toString());
                    statement.setLong(2, next);
                    statement.executeUpdate();
                }
                connection.commit();
                return next;
            } catch (final SQLException | ArithmeticException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (final SQLException | ArithmeticException exception) {
            throw failure("Unable to reserve a player snapshot revision", exception);
        }
    }

    synchronized StoredSnapshot storeNext(
        final UUID playerId,
        final String sourceBackendId,
        final byte[] snapshot
    ) throws IOException {
        validate(sourceBackendId, snapshot);
        try (Connection connection = this.connection()) {
            connection.setAutoCommit(false);
            try {
                final StoredSnapshot existing = find(connection, playerId);
                final long revision = existing == null ? 1 : Math.addExact(existing.revision(), 1);
                write(connection, playerId, revision, sourceBackendId, snapshot);
                connection.commit();
                return new StoredSnapshot(playerId, revision, sourceBackendId, snapshot);
            } catch (final SQLException | ArithmeticException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (final SQLException | ArithmeticException exception) {
            throw failure("Unable to assign a player snapshot revision", exception);
        }
    }

    synchronized StageResult acceptRevision(
        final UUID playerId,
        final long revision,
        final String sourceBackendId,
        final byte[] snapshot
    ) throws IOException {
        if (revision < 1) {
            throw new IOException("Player snapshot revision must be positive");
        }
        validate(sourceBackendId, snapshot);
        try (Connection connection = this.connection()) {
            connection.setAutoCommit(false);
            try {
                final StoredSnapshot existing = find(connection, playerId);
                if (existing != null && revision < existing.revision()) {
                    connection.rollback();
                    return StageResult.STALE;
                }
                if (existing != null && revision == existing.revision()) {
                    connection.rollback();
                    return existing.sourceBackendId().equals(sourceBackendId)
                        && Arrays.equals(existing.snapshot(), snapshot) ? StageResult.DUPLICATE : StageResult.CONFLICT;
                }
                write(connection, playerId, revision, sourceBackendId, snapshot);
                connection.commit();
                return StageResult.STORED;
            } catch (final SQLException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (final SQLException exception) {
            throw failure("Unable to stage a player snapshot revision", exception);
        }
    }

    synchronized Optional<StoredSnapshot> load(final UUID playerId) throws IOException {
        try (Connection connection = this.connection()) {
            return Optional.ofNullable(find(connection, playerId));
        } catch (final SQLException exception) {
            throw failure("Unable to load a player snapshot", exception);
        }
    }

    boolean awaitStage(
        final UUID playerId,
        final long revision,
        final String targetBackendId,
        final long timeoutMillis
    ) throws IOException {
        final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            final Optional<StoredSnapshot> stored = this.load(playerId);
            if (stored.isPresent() && stored.orElseThrow().revision() == revision) {
                final PlayerHandoffCodec.Stage stage = PlayerHandoffCodec.decodeStage(stored.orElseThrow().snapshot());
                if (targetBackendId.equals(stage.targetBackendId())) {
                    return true;
                }
            }
            try {
                java.util.concurrent.TimeUnit.MILLISECONDS.sleep(25);
            } catch (final InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while awaiting staged player state", exception);
            }
        }
        return false;
    }

    private Connection connection() throws SQLException {
        final Connection connection = DriverManager.getConnection(this.jdbcUrl);
        try (PreparedStatement statement = connection.prepareStatement("PRAGMA busy_timeout = 5000")) {
            statement.execute();
        }
        return connection;
    }

    private static StoredSnapshot find(final Connection connection, final UUID playerId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
            "SELECT revision, source_backend_id, snapshot FROM player_snapshots WHERE player_uuid = ?"
        )) {
            statement.setString(1, playerId.toString());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? new StoredSnapshot(
                    playerId,
                    result.getLong("revision"),
                    result.getString("source_backend_id"),
                    result.getBytes("snapshot")
                ) : null;
            }
        }
    }

    private static void write(
        final Connection connection,
        final UUID playerId,
        final long revision,
        final String sourceBackendId,
        final byte[] snapshot
    ) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO player_snapshots (player_uuid, revision, source_backend_id, snapshot, updated_epoch_ms)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(player_uuid) DO UPDATE SET
                revision = excluded.revision,
                source_backend_id = excluded.source_backend_id,
                snapshot = excluded.snapshot,
                updated_epoch_ms = excluded.updated_epoch_ms
            """)) {
            statement.setString(1, playerId.toString());
            statement.setLong(2, revision);
            statement.setString(3, sourceBackendId);
            statement.setBytes(4, snapshot);
            statement.setLong(5, System.currentTimeMillis());
            statement.executeUpdate();
        }
    }

    private static void validate(final String sourceBackendId, final byte[] snapshot) throws IOException {
        if (sourceBackendId == null || sourceBackendId.isBlank()) {
            throw new IOException("Player snapshot source backend ID is required");
        }
        if (snapshot == null || snapshot.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new IOException("Player snapshot exceeds the transport payload limit");
        }
    }

    private static IOException failure(final String message, final Exception cause) {
        return new IOException(message, cause);
    }

    enum StageResult {
        STORED,
        DUPLICATE,
        STALE,
        CONFLICT
    }

    record StoredSnapshot(UUID playerId, long revision, String sourceBackendId, byte[] snapshot) {
        StoredSnapshot {
            snapshot = snapshot.clone();
        }

        @Override
        public byte[] snapshot() {
            return this.snapshot.clone();
        }
    }
}
