package dev.shardingbase.velocity;

import dev.shardingbase.protocol.MapPlannerCodec;
import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** SQLite authority for map sessions, single-use browser credentials, tiles, and immutable cut plans. */
final class WorldPlannerStore {
    private final String jdbcUrl;
    private final SecureRandom random = new SecureRandom();

    WorldPlannerStore(final Path databasePath) throws IOException {
        this.jdbcUrl = "jdbc:sqlite:" + databasePath.toAbsolutePath().normalize();
        try (Connection connection = this.connection()) {
            connection.createStatement().executeUpdate("""
                CREATE TABLE IF NOT EXISTS map_sessions (
                    session_id TEXT PRIMARY KEY NOT NULL,
                    backend_id TEXT NOT NULL,
                    world_key TEXT NOT NULL,
                    world_directory TEXT NOT NULL,
                    world_id TEXT NOT NULL,
                    world_seed INTEGER NOT NULL,
                    data_version INTEGER NOT NULL,
                    min_chunk_x INTEGER NOT NULL,
                    max_chunk_x INTEGER NOT NULL,
                    min_chunk_z INTEGER NOT NULL,
                    max_chunk_z INTEGER NOT NULL,
                    generated_chunks INTEGER NOT NULL,
                    estimated_bytes INTEGER NOT NULL,
                    link_token_hash BLOB,
                    browser_token_hash BLOB,
                    state TEXT NOT NULL,
                    created_epoch_ms INTEGER NOT NULL
                )
                """);
            ensureColumn(connection, "map_sessions", "world_directory",
                "TEXT NOT NULL DEFAULT 'world'");
            ensureColumn(connection, "map_sessions", "world_id",
                "TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'");
            ensureColumn(connection, "map_sessions", "world_seed", "INTEGER NOT NULL DEFAULT 0");
            ensureColumn(connection, "map_sessions", "data_version", "INTEGER NOT NULL DEFAULT 1");
            connection.createStatement().executeUpdate("""
                CREATE TABLE IF NOT EXISTS map_tiles (
                    session_id TEXT NOT NULL,
                    tile_x INTEGER NOT NULL,
                    tile_z INTEGER NOT NULL,
                    png BLOB NOT NULL,
                    PRIMARY KEY (session_id, tile_x, tile_z),
                    FOREIGN KEY (session_id) REFERENCES map_sessions(session_id) ON DELETE CASCADE
                )
                """);
            connection.createStatement().executeUpdate("""
                CREATE TABLE IF NOT EXISTS world_transactions (
                    transaction_id TEXT PRIMARY KEY NOT NULL,
                    session_id TEXT UNIQUE NOT NULL,
                    axis TEXT NOT NULL,
                    cut_chunk INTEGER NOT NULL,
                    negative_backend_id TEXT NOT NULL,
                    positive_backend_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    updated_epoch_ms INTEGER NOT NULL,
                    created_epoch_ms INTEGER NOT NULL
                )
                """);
            ensureColumn(connection, "world_transactions", "detail", "TEXT NOT NULL DEFAULT ''");
            ensureColumn(connection, "world_transactions", "updated_epoch_ms", "INTEGER NOT NULL DEFAULT 0");
        } catch (final SQLException exception) {
            throw new IOException("Unable to initialize world planner storage", exception);
        }
    }

    synchronized void create(final MapPlannerCodec.Create session) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO map_sessions (
                session_id, backend_id, world_key, world_directory, world_id, world_seed, data_version,
                min_chunk_x, max_chunk_x, min_chunk_z, max_chunk_z, generated_chunks, estimated_bytes,
                state, created_epoch_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'UPLOADING', ?)
            """)) {
            statement.setString(1, session.sessionId().toString());
            statement.setString(2, session.backendId());
            statement.setString(3, session.worldKey());
            statement.setString(4, session.worldDirectory());
            statement.setString(5, session.worldId().toString());
            statement.setLong(6, session.worldSeed());
            statement.setInt(7, session.dataVersion());
            statement.setInt(8, session.minChunkX());
            statement.setInt(9, session.maxChunkX());
            statement.setInt(10, session.minChunkZ());
            statement.setInt(11, session.maxChunkZ());
            statement.setLong(12, session.generatedChunks());
            statement.setLong(13, session.estimatedBytes());
            statement.setLong(14, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (final SQLException exception) {
            throw new IOException("Unable to create map session", exception);
        }
    }

    synchronized void putTile(final MapPlannerCodec.Tile tile) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO map_tiles (session_id, tile_x, tile_z, png)
            SELECT ?, ?, ?, ? WHERE EXISTS (
                SELECT 1 FROM map_sessions WHERE session_id = ? AND state = 'UPLOADING'
            )
            ON CONFLICT(session_id, tile_x, tile_z) DO UPDATE SET png = excluded.png
            """)) {
            statement.setString(1, tile.sessionId().toString());
            statement.setInt(2, tile.tileX());
            statement.setInt(3, tile.tileZ());
            statement.setBytes(4, tile.png());
            statement.setString(5, tile.sessionId().toString());
            if (statement.executeUpdate() != 1) {
                throw new IOException("Map session is missing or no longer accepts tiles");
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to store map tile", exception);
        }
    }

    synchronized String complete(final UUID sessionId) throws IOException {
        final String token = token();
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement("""
            UPDATE map_sessions SET link_token_hash = ?, state = 'READY'
            WHERE session_id = ? AND state = 'UPLOADING'
            """)) {
            statement.setBytes(1, hash(token));
            statement.setString(2, sessionId.toString());
            if (statement.executeUpdate() != 1) {
                throw new IOException("Map session cannot be completed");
            }
            return token;
        } catch (final SQLException exception) {
            throw new IOException("Unable to complete map session", exception);
        }
    }

    synchronized Optional<Redeemed> redeem(final String linkToken) throws IOException {
        final String browserToken = token();
        try (Connection connection = this.connection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement find = connection.prepareStatement("""
                SELECT * FROM map_sessions WHERE link_token_hash = ? AND state = 'READY' LIMIT 1
                """)) {
                find.setBytes(1, hash(linkToken));
                try (ResultSet result = find.executeQuery()) {
                    if (!result.next()) {
                        connection.rollback();
                        return Optional.empty();
                    }
                    final Session session = session(result);
                    try (PreparedStatement update = connection.prepareStatement("""
                        UPDATE map_sessions SET browser_token_hash = ?, link_token_hash = NULL, state = 'REDEEMED'
                        WHERE session_id = ? AND state = 'READY'
                        """)) {
                        update.setBytes(1, hash(browserToken));
                        update.setString(2, session.sessionId().toString());
                        if (update.executeUpdate() != 1) {
                            connection.rollback();
                            return Optional.empty();
                        }
                    }
                    connection.commit();
                    return Optional.of(new Redeemed(session, browserToken));
                }
            } catch (final SQLException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to redeem planner link", exception);
        }
    }

    synchronized Optional<Session> authenticate(final UUID sessionId, final String browserToken) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement("""
            SELECT * FROM map_sessions
            WHERE session_id = ? AND browser_token_hash = ? AND state IN ('REDEEMED', 'CONFIRMED') LIMIT 1
            """)) {
            statement.setString(1, sessionId.toString());
            statement.setBytes(2, hash(browserToken));
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(session(result)) : Optional.empty();
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to authenticate planner session", exception);
        }
    }

    synchronized Optional<byte[]> tile(final UUID sessionId, final int tileX, final int tileZ) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT png FROM map_tiles WHERE session_id = ? AND tile_x = ? AND tile_z = ?"
        )) {
            statement.setString(1, sessionId.toString());
            statement.setInt(2, tileX);
            statement.setInt(3, tileZ);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(result.getBytes(1)) : Optional.empty();
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to load map tile", exception);
        }
    }

    synchronized Optional<String> backendId(final UUID sessionId) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT backend_id FROM map_sessions WHERE session_id = ?"
        )) {
            statement.setString(1, sessionId.toString());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(result.getString(1)) : Optional.empty();
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to identify map session owner", exception);
        }
    }

    synchronized List<TileCoordinate> tiles(final UUID sessionId) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT tile_x, tile_z FROM map_tiles WHERE session_id = ? ORDER BY tile_z, tile_x"
        )) {
            statement.setString(1, sessionId.toString());
            try (ResultSet result = statement.executeQuery()) {
                final List<TileCoordinate> values = new ArrayList<>();
                while (result.next()) {
                    values.add(new TileCoordinate(result.getInt(1), result.getInt(2)));
                }
                return List.copyOf(values);
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to list map tiles", exception);
        }
    }

    synchronized UUID confirm(
        final Session session,
        final String axis,
        final int cutChunk,
        final String negativeBackendId,
        final String positiveBackendId
    ) throws IOException {
        if (!("X".equals(axis) || "Z".equals(axis)) || negativeBackendId.equals(positiveBackendId)) {
            throw new IOException("Invalid world cut assignment");
        }
        final int minimum = "X".equals(axis) ? session.minChunkX() : session.minChunkZ();
        final int maximum = "X".equals(axis) ? session.maxChunkX() : session.maxChunkZ();
        if (cutChunk <= minimum || cutChunk > maximum) {
            throw new IOException("Cut must divide the generated chunk range");
        }
        final UUID transactionId = UUID.randomUUID();
        try (Connection connection = this.connection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement active = connection.prepareStatement(
                "SELECT COUNT(*) FROM world_transactions WHERE state NOT IN ('COMPLETE', 'ROLLED_BACK', 'FAILED')"
            ); ResultSet result = active.executeQuery()) {
                if (result.next() && result.getInt(1) != 0) {
                    connection.rollback();
                    throw new IOException("Another world transaction is already active");
                }
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO world_transactions (
                    transaction_id, session_id, axis, cut_chunk, negative_backend_id, positive_backend_id,
                    state, detail, updated_epoch_ms, created_epoch_ms
                ) VALUES (?, ?, ?, ?, ?, ?, 'PLANNED', 'awaiting preflight', ?, ?)
                """)) {
                insert.setString(1, transactionId.toString());
                insert.setString(2, session.sessionId().toString());
                insert.setString(3, axis);
                insert.setInt(4, cutChunk);
                insert.setString(5, negativeBackendId);
                insert.setString(6, positiveBackendId);
                final long now = System.currentTimeMillis();
                insert.setLong(7, now);
                insert.setLong(8, now);
                insert.executeUpdate();
            }
            try (PreparedStatement update = connection.prepareStatement(
                "UPDATE map_sessions SET state = 'CONFIRMED' WHERE session_id = ? AND state = 'REDEEMED'"
            )) {
                update.setString(1, session.sessionId().toString());
                if (update.executeUpdate() != 1) {
                    connection.rollback();
                    throw new IOException("Planner session was already confirmed");
                }
            }
            connection.commit();
            return transactionId;
        } catch (final SQLException exception) {
            throw new IOException("Unable to persist immutable world transaction plan", exception);
        }
    }

    synchronized Optional<TransactionPlan> transaction(final UUID transactionId) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement("""
            SELECT
                wt.transaction_id, wt.axis, wt.cut_chunk, wt.negative_backend_id, wt.positive_backend_id,
                wt.state AS transaction_state, wt.detail AS transaction_detail,
                ms.session_id, ms.backend_id, ms.world_key, ms.world_directory, ms.world_id, ms.world_seed,
                ms.data_version, ms.min_chunk_x, ms.max_chunk_x, ms.min_chunk_z, ms.max_chunk_z,
                ms.generated_chunks, ms.estimated_bytes, ms.state AS map_state
            FROM world_transactions wt
            JOIN map_sessions ms ON ms.session_id = wt.session_id
            WHERE wt.transaction_id = ?
            """)) {
            statement.setString(1, transactionId.toString());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(transactionPlan(result)) : Optional.empty();
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to load world transaction plan", exception);
        }
    }

    synchronized List<TransactionPlan> transactionsIn(final String... states) throws IOException {
        if (states.length == 0) {
            return List.of();
        }
        final String placeholders = String.join(",", java.util.Collections.nCopies(states.length, "?"));
        final String sql = """
            SELECT
                wt.transaction_id, wt.axis, wt.cut_chunk, wt.negative_backend_id, wt.positive_backend_id,
                wt.state AS transaction_state, wt.detail AS transaction_detail,
                ms.session_id, ms.backend_id, ms.world_key, ms.world_directory, ms.world_id, ms.world_seed,
                ms.data_version, ms.min_chunk_x, ms.max_chunk_x, ms.min_chunk_z, ms.max_chunk_z,
                ms.generated_chunks, ms.estimated_bytes, ms.state AS map_state
            FROM world_transactions wt
            JOIN map_sessions ms ON ms.session_id = wt.session_id
            WHERE wt.state IN (%s)
            ORDER BY wt.created_epoch_ms
            """.formatted(placeholders);
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < states.length; index++) {
                statement.setString(index + 1, states[index]);
            }
            try (ResultSet result = statement.executeQuery()) {
                final List<TransactionPlan> plans = new ArrayList<>();
                while (result.next()) {
                    plans.add(transactionPlan(result));
                }
                return List.copyOf(plans);
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to list recoverable world transactions", exception);
        }
    }

    synchronized void transition(
        final UUID transactionId,
        final String expectedState,
        final String nextState,
        final String detail
    ) throws IOException {
        if (expectedState == null || nextState == null || detail == null || detail.isBlank()) {
            throw new IllegalArgumentException("Transaction transition fields are required");
        }
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement("""
            UPDATE world_transactions
            SET state = ?, detail = ?, updated_epoch_ms = ?
            WHERE transaction_id = ? AND state = ?
            """)) {
            statement.setString(1, nextState);
            statement.setString(2, detail);
            statement.setLong(3, System.currentTimeMillis());
            statement.setString(4, transactionId.toString());
            statement.setString(5, expectedState);
            if (statement.executeUpdate() != 1) {
                throw new IOException("World transaction is no longer in expected state " + expectedState);
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to persist world transaction phase", exception);
        }
    }

    private Connection connection() throws SQLException {
        final Connection connection = DriverManager.getConnection(this.jdbcUrl);
        try (PreparedStatement statement = connection.prepareStatement("PRAGMA busy_timeout = 5000")) {
            statement.execute();
        }
        try (PreparedStatement statement = connection.prepareStatement("PRAGMA foreign_keys = ON")) {
            statement.execute();
        }
        return connection;
    }

    private static void ensureColumn(
        final Connection connection,
        final String table,
        final String column,
        final String declaration
    ) throws SQLException {
        try (
            PreparedStatement statement = connection.prepareStatement("PRAGMA table_info(" + table + ")");
            ResultSet result = statement.executeQuery()
        ) {
            while (result.next()) {
                if (column.equals(result.getString("name"))) {
                    return;
                }
            }
        }
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("ALTER TABLE " + table + " ADD COLUMN " + column + ' ' + declaration);
        }
    }

    private String token() {
        final byte[] bytes = new byte[32];
        this.random.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static byte[] hash(final String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (final java.security.NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static Session session(final ResultSet result) throws SQLException {
        return new Session(
            UUID.fromString(result.getString("session_id")),
            result.getString("backend_id"),
            result.getString("world_key"),
            result.getString("world_directory"),
            UUID.fromString(result.getString("world_id")),
            result.getLong("world_seed"),
            result.getInt("data_version"),
            result.getInt("min_chunk_x"),
            result.getInt("max_chunk_x"),
            result.getInt("min_chunk_z"),
            result.getInt("max_chunk_z"),
            result.getLong("generated_chunks"),
            result.getLong("estimated_bytes"),
            result.getString("state")
        );
    }

    private static TransactionPlan transactionPlan(final ResultSet result) throws SQLException {
        final Session session = new Session(
            UUID.fromString(result.getString("session_id")),
            result.getString("backend_id"),
            result.getString("world_key"),
            result.getString("world_directory"),
            UUID.fromString(result.getString("world_id")),
            result.getLong("world_seed"),
            result.getInt("data_version"),
            result.getInt("min_chunk_x"),
            result.getInt("max_chunk_x"),
            result.getInt("min_chunk_z"),
            result.getInt("max_chunk_z"),
            result.getLong("generated_chunks"),
            result.getLong("estimated_bytes"),
            result.getString("map_state")
        );
        return new TransactionPlan(
            UUID.fromString(result.getString("transaction_id")),
            session,
            result.getString("axis"),
            result.getInt("cut_chunk"),
            result.getString("negative_backend_id"),
            result.getString("positive_backend_id"),
            result.getString("transaction_state"),
            result.getString("transaction_detail")
        );
    }

    record Session(
        UUID sessionId,
        String backendId,
        String worldKey,
        String worldDirectory,
        UUID worldId,
        long worldSeed,
        int dataVersion,
        int minChunkX,
        int maxChunkX,
        int minChunkZ,
        int maxChunkZ,
        long generatedChunks,
        long estimatedBytes,
        String state
    ) {
    }

    record Redeemed(Session session, String browserToken) {
    }

    record TransactionPlan(
        UUID transactionId,
        Session session,
        String axis,
        int cutChunk,
        String negativeBackendId,
        String positiveBackendId,
        String state,
        String detail
    ) {
    }

    record TileCoordinate(int x, int z) {
    }
}
