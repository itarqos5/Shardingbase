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
                    last_seen_epoch_ms INTEGER NOT NULL
                )
                """)) {
                statement.executeUpdate();
            }
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
                        server_id, server_name, node_id, minecraft_version, shardingbase_version, status, last_seen_epoch_ms
                    ) VALUES (?, ?, ?, ?, ?, 'ONLINE', ?)
                    ON CONFLICT(server_id) DO UPDATE SET
                        server_name = excluded.server_name,
                        node_id = excluded.node_id,
                        minecraft_version = excluded.minecraft_version,
                        shardingbase_version = excluded.shardingbase_version,
                        status = 'ONLINE',
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
                    true,
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

    private Connection connection() throws SQLException {
        final Connection connection = DriverManager.getConnection(this.jdbcUrl);
        try (PreparedStatement statement = connection.prepareStatement("PRAGMA busy_timeout = 5000")) {
            statement.execute();
        }
        return connection;
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
}
