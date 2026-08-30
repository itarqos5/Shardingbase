package dev.shardingbase.velocity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.time.Instant;
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
    String transactionSigningKey,
    Set<String> remoteCommandAllowlist,
    String webBindAddress,
    int webPort,
    String webPublicUrl
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
        final boolean missingTransactionKey = !control.containsKey("transaction-signing-key");
        final String transactionSigningKey = missingTransactionKey
            ? token(32)
            : signingKey(control.get("transaction-signing-key"));
        final Map<?, ?> web = optionalMapping(root.get("web"), "web");
        final String webBind = web.isEmpty() ? "0.0.0.0" : string(web.get("bind"), "web.bind");
        final int webPort = web.isEmpty() ? 8080 : integer(web.get("port"), "web.port");
        if (webPort < 1 || webPort > 65_535) {
            throw new IOException("web.port is outside the valid port range");
        }
        final String webPublicUrl = web.isEmpty()
            ? "http://127.0.0.1:" + webPort
            : publicUrl(web.get("public-url"));
        final VelocityConfiguration configuration = new VelocityConfiguration(
            bind, port, keyStore, password, database, credentials, transactionSigningKey,
            stringSet(root.get("remote-command-allowlist"), "remote-command-allowlist"),
            webBind, webPort, webPublicUrl
        );
        if (missingTransactionKey) {
            final Path backup = path.resolveSibling(path.getFileName() + ".bak." + Instant.now().toEpochMilli());
            Files.copy(path, backup);
            write(path, configuration, normalizedDirectory, true);
        }
        return configuration;
    }

    private static VelocityConfiguration defaults(final Path directory) {
        return new VelocityConfiguration(
            "0.0.0.0",
            8443,
            directory.resolve("tls.p12"),
            token(24),
            directory.resolve("shardingbase.db"),
            Map.of("node-a", token(32), "node-b", token(32)),
            token(32),
            Set.of(),
            "0.0.0.0",
            8080,
            "http://127.0.0.1:8080"
        );
    }

    private static void write(final Path path, final VelocityConfiguration configuration, final Path directory)
        throws IOException {
        write(path, configuration, directory, false);
    }

    private static void write(
        final Path path,
        final VelocityConfiguration configuration,
        final Path directory,
        final boolean replace
    ) throws IOException {
        Files.createDirectories(directory);
        final Map<String, Object> root = new LinkedHashMap<>();
        final Map<String, Object> control = new LinkedHashMap<>();
        control.put("bind", configuration.bindAddress());
        control.put("port", configuration.controlPort());
        control.put("keystore", directory.relativize(configuration.keyStorePath()).toString().replace('\\', '/'));
        control.put("keystore-password", configuration.keyStorePassword());
        control.put("transaction-signing-key", configuration.transactionSigningKey());
        root.put("control", control);
        root.put("database", directory.relativize(configuration.databasePath()).toString().replace('\\', '/'));
        root.put("node-credentials", configuration.nodeCredentials());
        root.put("remote-command-allowlist", configuration.remoteCommandAllowlist().stream().sorted().toList());
        root.put("web", Map.of(
            "bind", configuration.webBindAddress(),
            "port", configuration.webPort(),
            "public-url", configuration.webPublicUrl()
        ));
        final DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        final String yaml = new Yaml(new Representer(options), options).dump(root);
        Path temporary = null;
        try {
            temporary = Files.createTempFile(directory, ".shardingbase-velocity-", ".tmp");
            Files.writeString(temporary, yaml, StandardCharsets.UTF_8);
            if (replace) {
                Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
            }
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

    private static String signingKey(final Object value) throws IOException {
        final String encoded = string(value, "control.transaction-signing-key");
        try {
            if (Base64.getUrlDecoder().decode(encoded).length < 32) {
                throw new IOException("control.transaction-signing-key must decode to at least 32 bytes");
            }
            return encoded;
        } catch (final IllegalArgumentException exception) {
            throw new IOException("control.transaction-signing-key must be URL-safe base64", exception);
        }
    }

    private static Map<?, ?> mapping(final Object value, final String field) throws IOException {
        if (value instanceof final Map<?, ?> map) {
            return map;
        }
        throw new IOException(field + " must be a mapping");
    }

    private static Map<?, ?> optionalMapping(final Object value, final String field) throws IOException {
        return value == null ? Map.of() : mapping(value, field);
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

    private static String publicUrl(final Object value) throws IOException {
        final String configured = string(value, "web.public-url");
        try {
            final URI uri = new URI(configured);
            if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null
                || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new IOException("web.public-url must be an absolute HTTP(S) URL without query or fragment");
            }
            return configured.endsWith("/") ? configured.substring(0, configured.length() - 1) : configured;
        } catch (final URISyntaxException exception) {
            throw new IOException("web.public-url is invalid", exception);
        }
    }
}
