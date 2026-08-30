package dev.shardingbase.velocity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.representer.Representer;

/**
 * Validated Velocity controller configuration.
 *
 * @param bindAddress      control listener bind address
 * @param controlPort      control listener port
 * @param keyStorePath     PKCS12 key store path
 * @param keyStorePassword key store password
 * @param databasePath     SQLite database path
 * @param nodeCredentials  exactly two node IDs and credentials
 */
record VelocityConfiguration(
    String bindAddress,
    int controlPort,
    Path keyStorePath,
    String keyStorePassword,
    Path databasePath,
    Map<String, String> nodeCredentials,
    Set<String> remoteCommandAllowlist
) {
    VelocityConfiguration {
        nodeCredentials = Map.copyOf(nodeCredentials);
        remoteCommandAllowlist = Set.copyOf(remoteCommandAllowlist);
    }

    static VelocityConfiguration load(final Path dataDirectory) throws IOException {
        final Path normalizedDirectory = dataDirectory.toAbsolutePath().normalize();
        final Path path = normalizedDirectory.resolve("config.yml");
        if (Files.notExists(path)) {
            final VelocityConfiguration generated = defaults(normalizedDirectory);
            write(path, generated, normalizedDirectory);
            return generated;
        }

        final LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        final Object document = new Yaml(new SafeConstructor(options)).load(Files.readString(path, StandardCharsets.UTF_8));
        final Map<?, ?> root = mapping(document, "root");
        final Map<?, ?> control = mapping(root.get("control"), "control");
        final String bind = string(control.get("bind"), "control.bind");
        final int port = integer(control.get("port"), "control.port");
        if (port < 1 || port > 65_535) {
            throw new IOException("control.port is outside the valid port range");
        }
        final Path keyStore = childPath(normalizedDirectory, string(control.get("keystore"), "control.keystore"));
        final String password = string(control.get("keystore-password"), "control.keystore-password");
        final Path database = childPath(normalizedDirectory, string(root.get("database"), "database"));
        final Map<?, ?> credentialsNode = mapping(root.get("node-credentials"), "node-credentials");
        if (credentialsNode.size() != 2) {
            throw new IOException("node-credentials must contain exactly two nodes");
        }
        final Map<String, String> credentials = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : credentialsNode.entrySet()) {
            credentials.put(string(entry.getKey(), "node id"), string(entry.getValue(), "node credential"));
        }
        return new VelocityConfiguration(
            bind, port, keyStore, password, database, credentials,
            stringSet(root.get("remote-command-allowlist"), "remote-command-allowlist")
        );
    }

    private static VelocityConfiguration defaults(final Path directory) {
        return new VelocityConfiguration(
            "0.0.0.0",
            8443,
            directory.resolve("tls.p12"),
            token(24),
            directory.resolve("shardingbase.db"),
            Map.of("node-a", token(32), "node-b", token(32)),
            Set.of()
        );
    }

    private static void write(final Path path, final VelocityConfiguration configuration, final Path directory)
        throws IOException {
        Files.createDirectories(directory);
        final Map<String, Object> root = new LinkedHashMap<>();
        final Map<String, Object> control = new LinkedHashMap<>();
        control.put("bind", configuration.bindAddress());
        control.put("port", configuration.controlPort());
        control.put("keystore", directory.relativize(configuration.keyStorePath()).toString().replace('\\', '/'));
        control.put("keystore-password", configuration.keyStorePassword());
        root.put("control", control);
        root.put("database", directory.relativize(configuration.databasePath()).toString().replace('\\', '/'));
        root.put("node-credentials", configuration.nodeCredentials());
        root.put("remote-command-allowlist", configuration.remoteCommandAllowlist().stream().sorted().toList());
        final DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        final String yaml = new Yaml(new Representer(options), options).dump(root);
        Path temporary = null;
        try {
            temporary = Files.createTempFile(directory, ".shardingbase-velocity-", ".tmp");
            Files.writeString(temporary, yaml, StandardCharsets.UTF_8);
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
            temporary = null;
        } catch (final AtomicMoveNotSupportedException exception) {
            throw new IOException("Atomic configuration creation is not supported", exception);
        } finally {
            if (temporary != null) {
                Files.deleteIfExists(temporary);
            }
        }
    }

    private static Path childPath(final Path directory, final String configured) throws IOException {
        final Path path = directory.resolve(configured).toAbsolutePath().normalize();
        if (!path.startsWith(directory)) {
            throw new IOException("Configured path escapes the Shardingbase plugin directory: " + configured);
        }
        return path;
    }

    private static String token(final int bytes) {
        final byte[] random = new byte[bytes];
        new SecureRandom().nextBytes(random);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(random);
    }

    private static Map<?, ?> mapping(final Object value, final String field) throws IOException {
        if (value instanceof final Map<?, ?> map) {
            return map;
        }
        throw new IOException(field + " must be a mapping");
    }

    private static String string(final Object value, final String field) throws IOException {
        if (value instanceof final String string && !string.isBlank()) {
            return string;
        }
        throw new IOException(field + " must be a non-empty string");
    }

    private static int integer(final Object value, final String field) throws IOException {
        if (value instanceof final Number number) {
            return number.intValue();
        }
        throw new IOException(field + " must be an integer");
    }

    private static Set<String> stringSet(final Object value, final String field) throws IOException {
        if (value == null) {
            return Set.of();
        }
        if (!(value instanceof final List<?> values)) {
            throw new IOException(field + " must be a list");
        }
        final Set<String> result = new LinkedHashSet<>();
        for (final Object entry : values) {
            final String label = string(entry, field + " entry").toLowerCase(Locale.ROOT);
            if (!label.matches("[a-z0-9_.:-]+")) {
                throw new IOException(field + " contains an invalid command label: " + label);
            }
            result.add(label);
        }
        return Set.copyOf(result);
    }
}
