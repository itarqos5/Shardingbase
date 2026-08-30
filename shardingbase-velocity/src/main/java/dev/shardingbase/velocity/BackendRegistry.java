package dev.shardingbase.velocity;

import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationRequest;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.ArrayList;
import java.util.List;

/** Transactional SQLite authority for the exactly-two-backend prototype. */
final class BackendRegistry {
    private final String jdbcUrl;

    BackendRegistry(final Path databasePath) throws IOException {
        Files.createDirectories(databasePath.getParent());
        this.jdbcUrl = "jdbc:sqlite:" + databasePath.toAbsolutePath().normalize();
        try (Connection connection = this.connection()) {
            try (PreparedStatement statement = connection.prepareStatement("""
                CREATE TABLE IF NOT EXISTS backends (
                    server_id TEXT PRIMARY KEY NOT NULL,
                    server_name TEXT UNIQUE NOT NULL,
                    node_id TEXT UNIQUE NOT NULL,
                    minecraft_version TEXT NOT NULL,
                    shardingbase_version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    status_detail TEXT NOT NULL,
                    last_seen_epoch_ms INTEGER NOT NULL
                )
                """)) {
                statement.executeUpdate();
            }
            ensureColumn(connection, "backends", "status_detail", "TEXT NOT NULL DEFAULT ''");
        } catch (final SQLException exception) {
            throw new IOException("Unable to initialize " + databasePath, exception);
        }
    }

    synchronized ValidationResponse register(final String nodeId, final ValidationRequest request) throws IOException {
        try (Connection connection = this.connection()) {
            connection.setAutoCommit(false);
            try {
                final Existing identity = find(connection, "server_id", request.serverId());
                if (identity != null && !identity.matches(nodeId, request)) {
                    return rollback(connection, "server-id is already registered to a different backend or node");
                }
                final Existing name = find(connection, "server_name", request.serverName());
                if (name != null && !name.matches(nodeId, request)) {
                    return rollback(connection, "server-name is already registered to a different backend or node");
                }
                final Existing node = find(connection, "node_id", nodeId);
                if (node != null && !node.matches(nodeId, request)) {
                    return rollback(connection, "node credential is already bound to another backend");
                }

                final Existing versionPeer = first(connection);
                if (versionPeer != null && (!versionPeer.minecraftVersion().equals(request.minecraftVersion())
                    || !versionPeer.shardingbaseVersion().equals(request.shardingbaseVersion()))) {
                    return rollback(connection, "backend Minecraft/Shardingbase versions do not match the registered pair");
                }
                if (identity == null && count(connection) >= 2) {
                    return rollback(connection, "this prototype accepts exactly two backends");
                }

                try (PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO backends (
                        server_id, server_name, node_id, minecraft_version, shardingbase_version,
                        status, status_detail, last_seen_epoch_ms
                    ) VALUES (?, ?, ?, ?, ?, 'ONLINE', 'validated', ?)
                    ON CONFLICT(server_id) DO UPDATE SET
                        server_name = excluded.server_name,
                        node_id = excluded.node_id,
                        minecraft_version = excluded.minecraft_version,
                        shardingbase_version = excluded.shardingbase_version,
                        status = CASE
                            WHEN backends.status = 'MAINTENANCE' THEN backends.status
                            ELSE 'ONLINE'
                        END,
                        status_detail = CASE
                            WHEN backends.status = 'MAINTENANCE' THEN backends.status_detail
                            ELSE 'validated'
                        END,
                        last_seen_epoch_ms = excluded.last_seen_epoch_ms
                    """)) {
                    statement.setString(1, request.serverId());
                    statement.setString(2, request.serverName());
                    statement.setString(3, nodeId);
                    statement.setString(4, request.minecraftVersion());
                    statement.setString(5, request.shardingbaseVersion());
                    statement.setLong(6, System.currentTimeMillis());
                    statement.executeUpdate();
                }
                final Existing peer = peer(connection, request.serverId());
                connection.commit();
                final String detail = peer == null
                    ? "validated; waiting for the second backend"
                    : "validated with peer " + peer.serverName();
                return new ValidationResponse(
                    peer != null,
                    detail,
                    peer == null ? "" : peer.serverId(),
                    peer == null ? "" : peer.serverName()
                );
            } catch (final SQLException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (final SQLException exception) {
            throw new IOException("SQLite backend registration failed", exception);
        }
    }

    synchronized Optional<String> nodeIdForTarget(final String targetId) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT node_id FROM backends WHERE server_id = ? OR server_name = ? OR node_id = ? LIMIT 1"
        )) {
            statement.setString(1, targetId);
            statement.setString(2, targetId);
            statement.setString(3, targetId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(result.getString("node_id")) : Optional.empty();
            }
        } catch (final SQLException exception) {
            throw new IOException("SQLite backend target lookup failed", exception);
        }
    }

    synchronized Optional<BackendTarget> backendForName(final String serverName) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT server_id, server_name, node_id FROM backends WHERE server_name = ? LIMIT 1"
        )) {
            statement.setString(1, serverName);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(new BackendTarget(
                    result.getString("server_id"),
                    result.getString("server_name"),
                    result.getString("node_id")
                )) : Optional.empty();
            }
        } catch (final SQLException exception) {
            throw new IOException("SQLite backend lookup failed", exception);
        }
    }

    synchronized Optional<BackendTarget> peerForName(final String serverName) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT server_id, server_name, node_id FROM backends WHERE server_name <> ? ORDER BY server_id LIMIT 1"
        )) {
            statement.setString(1, serverName);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(new BackendTarget(
                    result.getString("server_id"),
                    result.getString("server_name"),
                    result.getString("node_id")
                )) : Optional.empty();
            }
        } catch (final SQLException exception) {
            throw new IOException("SQLite peer backend lookup failed", exception);
        }
    }

    synchronized List<BackendTarget> backends() throws IOException {
        try (
            Connection connection = this.connection();
            PreparedStatement statement = connection.prepareStatement(
                "SELECT server_id, server_name, node_id FROM backends ORDER BY server_name"
            );
            ResultSet result = statement.executeQuery()
        ) {
            final List<BackendTarget> backends = new ArrayList<>();
            while (result.next()) {
                backends.add(new BackendTarget(
                    result.getString("server_id"),
                    result.getString("server_name"),
                    result.getString("node_id")
                ));
            }
            return List.copyOf(backends);
        } catch (final SQLException exception) {
            throw new IOException("SQLite backend list lookup failed", exception);
        }
    }

    synchronized Optional<BackendStatus> statusForName(final String serverName) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT server_id, server_name, status, status_detail FROM backends WHERE server_name = ? LIMIT 1"
        )) {
            statement.setString(1, serverName);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(new BackendStatus(
                    result.getString("server_id"),
                    result.getString("server_name"),
                    result.getString("status"),
                    result.getString("status_detail")
                )) : Optional.empty();
            }
        } catch (final SQLException exception) {
            throw new IOException("SQLite backend status lookup failed", exception);
        }
    }

    synchronized long lastSeen(final String serverId) throws IOException {
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "SELECT last_seen_epoch_ms FROM backends WHERE server_id = ? LIMIT 1"
        )) {
            statement.setString(1, serverId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IOException("Backend is not registered: " + serverId);
                }
                return result.getLong(1);
            }
        } catch (final SQLException exception) {
            throw new IOException("SQLite backend health lookup failed", exception);
        }
    }

    synchronized void setPairStatus(
        final List<String> serverIds,
        final String status,
        final String detail
    ) throws IOException {
        if (serverIds.size() != 2 || serverIds.get(0).equals(serverIds.get(1))
            || !status.matches("[A-Z_]{2,32}") || detail == null || detail.isBlank()) {
            throw new IllegalArgumentException("Exactly two distinct backends and a status detail are required");
        }
        try (Connection connection = this.connection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE backends SET status = ?, status_detail = ? WHERE server_id = ?"
            )) {
                int updated = 0;
                for (final String serverId : serverIds) {
                    statement.setString(1, status);
                    statement.setString(2, detail);
                    statement.setString(3, serverId);
                    updated += statement.executeUpdate();
                }
                if (updated != 2) {
                    connection.rollback();
                    throw new IOException("Both transaction backends must be registered before changing status");
                }
                connection.commit();
            } catch (final SQLException | IOException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to update backend maintenance state", exception);
        }
    }

    synchronized void clearPairHealth(final List<String> serverIds) throws IOException {
        if (serverIds.size() != 2 || serverIds.get(0).equals(serverIds.get(1))) {
            throw new IllegalArgumentException("Exactly two distinct backend IDs are required");
        }
        try (Connection connection = this.connection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE backends SET last_seen_epoch_ms = 0 WHERE server_id = ?"
            )) {
                int updated = 0;
                for (final String serverId : serverIds) {
                    statement.setString(1, serverId);
                    updated += statement.executeUpdate();
                }
                if (updated != 2) {
                    connection.rollback();
                    throw new IOException("Both backends must be registered before clearing health");
                }
                connection.commit();
            } catch (final SQLException | IOException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to clear backend health state", exception);
        }
    }

    synchronized void clearHealth(final String serverId) throws IOException {
        if (serverId == null || serverId.isBlank()) {
            throw new IllegalArgumentException("Backend ID is required");
        }
        try (Connection connection = this.connection(); PreparedStatement statement = connection.prepareStatement(
            "UPDATE backends SET last_seen_epoch_ms = 0 WHERE server_id = ?"
        )) {
            statement.setString(1, serverId);
            if (statement.executeUpdate() != 1) {
                throw new IOException("Backend health reset did not match exactly one backend");
            }
        } catch (final SQLException exception) {
            throw new IOException("Unable to clear backend health", exception);
        }
    }

    private Connection connection() throws SQLException {
        final Connection connection = DriverManager.getConnection(this.jdbcUrl);
        try (PreparedStatement statement = connection.prepareStatement("PRAGMA busy_timeout = 5000")) {
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

    private static Existing find(final Connection connection, final String column, final String value) throws SQLException {
        final String sql = "SELECT server_id, server_name, node_id, minecraft_version, shardingbase_version FROM backends WHERE "
            + column + " = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, value);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? existing(result) : null;
            }
        }
    }

    private static Existing first(final Connection connection) throws SQLException {
        try (
            PreparedStatement statement = connection.prepareStatement(
                "SELECT server_id, server_name, node_id, minecraft_version, shardingbase_version FROM backends LIMIT 1"
            );
            ResultSet result = statement.executeQuery()
        ) {
            return result.next() ? existing(result) : null;
        }
    }

    private static Existing peer(final Connection connection, final String serverId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
            "SELECT server_id, server_name, node_id, minecraft_version, shardingbase_version FROM backends WHERE server_id <> ? LIMIT 1"
        )) {
            statement.setString(1, serverId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? existing(result) : null;
            }
        }
    }

    private static int count(final Connection connection) throws SQLException {
        try (
            PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM backends");
            ResultSet result = statement.executeQuery()
        ) {
            return result.next() ? result.getInt(1) : 0;
        }
    }

    private static Existing existing(final ResultSet result) throws SQLException {
        return new Existing(
            result.getString("server_id"),
            result.getString("server_name"),
            result.getString("node_id"),
            result.getString("minecraft_version"),
            result.getString("shardingbase_version")
        );
    }

    private static ValidationResponse rollback(final Connection connection, final String detail) throws SQLException {
        connection.rollback();
        return new ValidationResponse(false, detail, "", "");
    }

    private record Existing(
        String serverId,
        String serverName,
        String nodeId,
        String minecraftVersion,
        String shardingbaseVersion
    ) {
        private boolean matches(final String candidateNodeId, final ValidationRequest request) {
            return this.serverId.equals(request.serverId())
                && this.serverName.equals(request.serverName())
                && this.nodeId.equals(candidateNodeId);
        }
    }

    record BackendTarget(String serverId, String serverName, String nodeId) {
    }

    record BackendStatus(String serverId, String serverName, String status, String detail) {
        boolean maintenance() {
            return "MAINTENANCE".equals(this.status);
        }
    }
}
