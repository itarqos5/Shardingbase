package dev.shardingbase.server.config;

import dev.shardingbase.api.ServerIdentity;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

/** Loads and safely repairs {@code config/shardingbase.yml}. */
public final class ShardingbaseConfigurationLoader {
    private static final Set<String> KEYS = Set.of("server-id", "server-name");
    private static final DateTimeFormatter BACKUP_TIME = DateTimeFormatter
        .ofPattern("uuuuMMdd-HHmmss-SSS")
        .withZone(ZoneOffset.UTC);

    private final Path path;
    private final Clock clock;
    private final Supplier<UUID> uuidSupplier;

    /**
     * Creates a loader for the given identity file.
     *
     * @param path configuration path
     */
    public ShardingbaseConfigurationLoader(final Path path) {
        this(path, Clock.systemUTC(), UUID::randomUUID);
    }

    ShardingbaseConfigurationLoader(final Path path, final Clock clock, final Supplier<UUID> uuidSupplier) {
        this.path = path.toAbsolutePath().normalize();
        this.clock = clock;
        this.uuidSupplier = uuidSupplier;
    }

    /**
     * Loads the current identity, creating or repairing the file when safe.
     *
     * @return validated configuration
     * @throws ShardingbaseConfigurationException when the file cannot be safely interpreted or written
     */
    public ShardingbaseConfiguration load() throws ShardingbaseConfigurationException {
        if (Files.notExists(this.path)) {
            final ShardingbaseConfiguration generated = new ShardingbaseConfiguration(
                new ServerIdentity(this.uuidSupplier.get().toString(), "change-me")
            );
            this.writeAtomically(generated);
            return generated;
        }

        final Map<String, Object> root = this.readRoot();
        final ParsedScalar serverId = scalar(root, "server-id");
        final ParsedScalar serverName = scalar(root, "server-name");
        boolean repair = !root.keySet().equals(KEYS) || serverId.coerced() || serverName.coerced();

        String id = serverId.value();
        if (id == null || id.isBlank()) {
            id = this.uuidSupplier.get().toString();
            repair = true;
        }

        String name = serverName.value();
        if (name == null) {
            name = "change-me";
            repair = true;
        }

        final ShardingbaseConfiguration configuration = new ShardingbaseConfiguration(new ServerIdentity(id, name));
        if (repair) {
            this.backUp();
            this.writeAtomically(configuration);
        }
        return configuration;
    }

    private Map<String, Object> readRoot() throws ShardingbaseConfigurationException {
        final LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        final Yaml yaml = new Yaml(new SafeConstructor(options));
        final Object loaded;
        try {
            loaded = yaml.load(Files.readString(this.path, StandardCharsets.UTF_8));
        } catch (final IOException | YAMLException exception) {
            throw new ShardingbaseConfigurationException(
                "Unable to parse " + this.path + "; duplicate keys or invalid YAML are fatal",
                exception
            );
        }

        if (loaded == null) {
            return new LinkedHashMap<>();
        }
        if (!(loaded instanceof final Map<?, ?> input)) {
            throw new ShardingbaseConfigurationException(this.path + " must contain a YAML mapping");
        }

        final Map<String, Object> result = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : input.entrySet()) {
            if (!(entry.getKey() instanceof final String key)) {
                throw new ShardingbaseConfigurationException(this.path + " contains a non-string key");
            }
            result.put(key, entry.getValue());
        }
        return result;
    }

    private static ParsedScalar scalar(final Map<String, Object> root, final String key)
        throws ShardingbaseConfigurationException {
        final Object value = root.get(key);
        if (value == null || value instanceof String) {
            return new ParsedScalar((String) value, false);
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof Character) {
            return new ParsedScalar(String.valueOf(value), true);
        }
        throw new ShardingbaseConfigurationException(key + " must be a scalar value");
    }

    private void backUp() throws ShardingbaseConfigurationException {
        final String suffix = ".backup-" + BACKUP_TIME.format(this.clock.instant());
        Path backup = this.path.resolveSibling(this.path.getFileName() + suffix);
        int collision = 0;
        while (Files.exists(backup)) {
            collision++;
            backup = this.path.resolveSibling(this.path.getFileName() + suffix + '-' + collision);
        }
        try {
            Files.copy(this.path, backup, StandardCopyOption.COPY_ATTRIBUTES);
        } catch (final IOException exception) {
            throw new ShardingbaseConfigurationException("Unable to back up " + this.path + " to " + backup, exception);
        }
    }

    private void writeAtomically(final ShardingbaseConfiguration configuration)
        throws ShardingbaseConfigurationException {
        final Path parent = this.path.getParent();
        if (parent == null) {
            throw new ShardingbaseConfigurationException("Configuration path has no parent: " + this.path);
        }

        Path temporary = null;
        try {
            Files.createDirectories(parent);
            temporary = Files.createTempFile(parent, ".shardingbase-", ".yml.tmp");
            final byte[] bytes = serialize(configuration).getBytes(StandardCharsets.UTF_8);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                final ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(temporary, this.path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            temporary = null;
        } catch (final AtomicMoveNotSupportedException exception) {
            throw new ShardingbaseConfigurationException(
                "Atomic configuration replacement is not supported for " + this.path,
                exception
            );
        } catch (final IOException exception) {
            throw new ShardingbaseConfigurationException("Unable to write " + this.path, exception);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (final IOException ignored) {
                    // The original failure remains authoritative.
                }
            }
        }
    }

    private static String serialize(final ShardingbaseConfiguration configuration) {
        return "server-id: \"" + escape(configuration.identity().serverId()) + "\"\n"
            + "server-name: \"" + escape(configuration.identity().serverName()) + "\"\n";
    }

    private static String escape(final String value) {
        final StringBuilder escaped = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            final char character = value.charAt(index);
            switch (character) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> escaped.append(character);
            }
        }
        return escaped.toString();
    }

    private record ParsedScalar(@Nullable String value, boolean coerced) {
    }
}
