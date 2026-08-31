package dev.shardingbase.velocity;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Loads the shaded SQLite driver through Velocity's isolated plugin class loader. */
final class SqliteSupport {
    private SqliteSupport() {
    }

    static void ensureDriverLoaded() throws IOException {
        try {
            Class.forName("org.sqlite.JDBC", true, SqliteSupport.class.getClassLoader());
        } catch (final ClassNotFoundException exception) {
            throw new IOException("The bundled SQLite JDBC driver is unavailable", exception);
        }
    }

    static Connection open(final String jdbcUrl) throws SQLException {
        final Connection connection = DriverManager.getConnection(jdbcUrl);
        boolean configured = false;
        try (var statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout = 5000");
            statement.execute("PRAGMA foreign_keys = ON");
            configured = true;
            return connection;
        } finally {
            if (!configured) {
                connection.close();
            }
        }
    }
}
