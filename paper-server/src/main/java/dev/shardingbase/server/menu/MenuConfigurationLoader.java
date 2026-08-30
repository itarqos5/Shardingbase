package dev.shardingbase.server.menu;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bukkit.Material;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.representer.Representer;

/** Loads independently recoverable menu layout files. */
public final class MenuConfigurationLoader {
    private static final DateTimeFormatter BACKUP_TIME = DateTimeFormatter
        .ofPattern("uuuuMMdd-HHmmss-SSS")
        .withZone(ZoneOffset.UTC);

    private final Path directory;
    private final Logger logger;
    private final Clock clock;

    public MenuConfigurationLoader(final Path directory, final Logger logger) {
        this(directory, logger, Clock.systemUTC());
    }

    MenuConfigurationLoader(final Path directory, final Logger logger, final Clock clock) {
        this.directory = directory.toAbsolutePath().normalize();
        this.logger = logger;
        this.clock = clock;
    }

    /** Loads every menu, falling back per-file when one is invalid. */
    public Map<String, MenuDefinition> load() {
        final Map<String, MenuDefinition> defaults = DefaultMenus.all();
        final Map<String, MenuDefinition> loaded = new LinkedHashMap<>();
        for (final Map.Entry<String, MenuDefinition> entry : defaults.entrySet()) {
            loaded.put(entry.getKey(), this.loadOne(entry.getValue()));
        }
        return Map.copyOf(loaded);
    }

    private MenuDefinition loadOne(final MenuDefinition fallback) {
        final Path path = this.directory.resolve(fallback.id() + ".yml");
        if (Files.notExists(path)) {
            try {
                this.write(path, fallback);
            } catch (final IOException exception) {
                this.logger.log(Level.WARNING, "Unable to create " + path + "; using the built-in menu", exception);
            }
            return fallback;
        }

        try {
            return parse(path, fallback.id());
        } catch (final IOException | IllegalArgumentException | YAMLException exception) {
            this.logger.log(Level.WARNING, "Invalid Shardingbase menu " + path + "; using and restoring its built-in default", exception);
            try {
                this.backUp(path);
                this.write(path, fallback);
            } catch (final IOException repairFailure) {
                this.logger.log(Level.WARNING, "Unable to back up or repair " + path + "; the in-memory default remains active", repairFailure);
            }
            return fallback;
        }
    }

    private static MenuDefinition parse(final Path path, final String id) throws IOException {
        final LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        final Object document = new Yaml(new SafeConstructor(options)).load(Files.readString(path, StandardCharsets.UTF_8));
        final Map<?, ?> root = mapping(document, "menu root");
        final String title = string(root.get("title"), "title");
        final int rows = integer(root.get("rows"), "rows");
        if (rows < 1 || rows > 6) {
            throw new IllegalArgumentException("rows must be between 1 and 6");
        }
        final Map<?, ?> buttonMap = mapping(root.get("buttons"), "buttons");
        final Map<String, MenuButton> buttons = new LinkedHashMap<>();
        final Set<Integer> occupiedSlots = new HashSet<>();
        for (final Map.Entry<?, ?> entry : buttonMap.entrySet()) {
            final String buttonId = string(entry.getKey(), "button id");
            final Map<?, ?> node = mapping(entry.getValue(), "button " + buttonId);
            final boolean enabled = bool(node.get("enabled"), buttonId + ".enabled");
            final Material material = Material.matchMaterial(string(node.get("material"), buttonId + ".material"));
            if (material == null || material == Material.AIR || material == Material.CAVE_AIR || material == Material.VOID_AIR) {
                throw new IllegalArgumentException(buttonId + ".material is not a usable material");
            }
            final int slot = integer(node.get("slot"), buttonId + ".slot");
            if (slot < 0 || slot >= rows * 9 || !occupiedSlots.add(slot)) {
                throw new IllegalArgumentException(buttonId + ".slot is outside the menu or already occupied");
            }
            final String name = string(node.get("name"), buttonId + ".name");
            final List<String> lore = strings(node.get("lore"), buttonId + ".lore");
            buttons.put(buttonId, new MenuButton(buttonId, enabled, material, slot, name, lore));
        }
        return new MenuDefinition(id, title, rows, buttons);
    }

    private void write(final Path path, final MenuDefinition definition) throws IOException {
        Files.createDirectories(path.getParent());
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("title", definition.title());
        root.put("rows", definition.rows());
        final Map<String, Object> buttons = new LinkedHashMap<>();
        for (final MenuButton button : definition.buttons().values()) {
            final Map<String, Object> node = new LinkedHashMap<>();
            node.put("enabled", button.enabled());
            node.put("material", button.material().name());
            node.put("slot", button.slot());
            node.put("name", button.name());
            node.put("lore", button.lore());
            buttons.put(button.id(), node);
        }
        root.put("buttons", buttons);

        final DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        final Representer representer = new Representer(options);
        final String serialized = new Yaml(representer, options).dump(root);
        Path temporary = null;
        try {
            temporary = Files.createTempFile(path.getParent(), ".shardingbase-menu-", ".tmp");
            Files.writeString(temporary, serialized, StandardCharsets.UTF_8);
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            temporary = null;
        } catch (final AtomicMoveNotSupportedException exception) {
            throw new IOException("Atomic menu replacement is not supported for " + path, exception);
        } finally {
            if (temporary != null) {
                Files.deleteIfExists(temporary);
            }
        }
    }

    private void backUp(final Path path) throws IOException {
        final String suffix = ".backup-" + BACKUP_TIME.format(this.clock.instant());
        Path backup = path.resolveSibling(path.getFileName() + suffix);
        int collision = 0;
        while (Files.exists(backup)) {
            backup = path.resolveSibling(path.getFileName() + suffix + '-' + ++collision);
        }
        Files.copy(path, backup, StandardCopyOption.COPY_ATTRIBUTES);
    }

    private static Map<?, ?> mapping(final Object value, final String field) {
        if (value instanceof final Map<?, ?> map) {
            return map;
        }
        throw new IllegalArgumentException(field + " must be a mapping");
    }

    private static String string(final Object value, final String field) {
        if (value instanceof final String string && !string.isBlank()) {
            return string;
        }
        throw new IllegalArgumentException(field + " must be a non-empty string");
    }

    private static int integer(final Object value, final String field) {
        if (value instanceof final Number number) {
            return number.intValue();
        }
        throw new IllegalArgumentException(field + " must be an integer");
    }

    private static boolean bool(final Object value, final String field) {
        if (value instanceof final Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException(field + " must be true or false");
    }

    private static List<String> strings(final Object value, final String field) {
        if (!(value instanceof final List<?> list)) {
            throw new IllegalArgumentException(field + " must be a list of strings");
        }
        final List<String> strings = new ArrayList<>(list.size());
        for (final Object element : list) {
            strings.add(string(element, field));
        }
        return strings;
    }
}
