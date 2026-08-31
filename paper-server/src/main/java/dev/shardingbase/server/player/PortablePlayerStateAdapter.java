package dev.shardingbase.server.player;

import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerSnapshot;
import dev.shardingbase.protocol.ProtocolException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bukkit.Bukkit;
import org.bukkit.GameMode;
import org.bukkit.Material;
import org.bukkit.NamespacedKey;
import org.bukkit.Registry;
import org.bukkit.Statistic;
import org.bukkit.advancement.AdvancementProgress;
import org.bukkit.advancement.Advancement;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.potion.PotionEffect;
import org.bukkit.potion.PotionEffectType;

/** Captures and merges only explicitly portable player fields. */
public final class PortablePlayerStateAdapter {
    private static final int MAX_COLLECTION_ENTRIES = 100_000;
    private static final int MAX_STRING_BYTES = 32_768;

    /** Captures the selected categories without routing-owned fields. */
    public PlayerSnapshot capture(
        final Player player,
        final long revision,
        final String sourceBackendId,
        final Set<PlayerDataCategory> selected
    ) throws IOException {
        final EnumMap<PlayerDataCategory, byte[]> categories = new EnumMap<>(PlayerDataCategory.class);
        for (final PlayerDataCategory category : selected) {
            categories.put(category, this.captureCategory(player, category));
        }
        return new PlayerSnapshot(player.getUniqueId(), revision, sourceBackendId, categories);
    }

    /** Applies every category in the snapshot, leaving all absent and routing-owned fields untouched. */
    public void apply(final Player player, final PlayerSnapshot snapshot) throws IOException {
        if (!player.getUniqueId().equals(snapshot.playerId())) {
            throw new IOException("Player snapshot UUID does not match the target player");
        }
        try {
            for (final Map.Entry<PlayerDataCategory, byte[]> entry : snapshot.categories().entrySet()) {
                this.applyCategory(player, entry.getKey(), entry.getValue());
            }
            player.updateInventory();
        } catch (final IllegalArgumentException | IllegalStateException exception) {
            throw new ProtocolException("Invalid portable player state", exception);
        }
    }

    private byte[] captureCategory(final Player player, final PlayerDataCategory category) throws IOException {
        return switch (category) {
            case INVENTORY -> ItemStack.serializeItemsAsBytes(player.getInventory().getStorageContents());
            case ARMOR -> ItemStack.serializeItemsAsBytes(player.getInventory().getArmorContents());
            case OFFHAND -> ItemStack.serializeItemsAsBytes(new ItemStack[] {
                player.getInventory().getItemInOffHand()
            });
            case SELECTED_SLOT -> encode(output -> output.writeInt(player.getInventory().getHeldItemSlot()));
            case ENDER_CHEST -> ItemStack.serializeItemsAsBytes(player.getEnderChest().getContents());
            case EXPERIENCE -> encode(output -> {
                output.writeFloat(player.getExp());
                output.writeInt(player.getLevel());
                output.writeInt(player.getTotalExperience());
            });
            case HEALTH -> encode(output -> {
                output.writeDouble(player.getHealth());
                output.writeDouble(player.getAbsorptionAmount());
            });
            case FOOD -> encode(output -> {
                output.writeInt(player.getFoodLevel());
                output.writeFloat(player.getSaturation());
                output.writeFloat(player.getExhaustion());
            });
            case POTION_EFFECTS -> capturePotionEffects(player.getActivePotionEffects());
            case GAME_MODE -> encode(output -> writeString(output, player.getGameMode().name()));
            case ABILITIES -> encode(output -> {
                output.writeBoolean(player.getAllowFlight());
                output.writeBoolean(player.isFlying());
                output.writeFloat(player.getFlySpeed());
                output.writeFloat(player.getWalkSpeed());
            });
            case ADVANCEMENTS -> captureAdvancements(player);
            case STATISTICS -> captureStatistics(player);
        };
    }

    private void applyCategory(final Player player, final PlayerDataCategory category, final byte[] payload)
        throws IOException {
        switch (category) {
            case INVENTORY -> player.getInventory().setStorageContents(ItemStack.deserializeItemsFromBytes(payload));
            case ARMOR -> player.getInventory().setArmorContents(ItemStack.deserializeItemsFromBytes(payload));
            case OFFHAND -> {
                final ItemStack[] items = ItemStack.deserializeItemsFromBytes(payload);
                if (items.length != 1) {
                    throw new ProtocolException("Offhand payload must contain exactly one slot");
                }
                player.getInventory().setItemInOffHand(items[0]);
            }
            case SELECTED_SLOT -> decode(payload, input -> {
                player.getInventory().setHeldItemSlot(input.readInt());
            });
            case ENDER_CHEST -> player.getEnderChest().setContents(ItemStack.deserializeItemsFromBytes(payload));
            case EXPERIENCE -> decode(payload, input -> {
                player.setExp(input.readFloat());
                player.setLevel(input.readInt());
                player.setTotalExperience(input.readInt());
            });
            case HEALTH -> decode(payload, input -> {
                player.setHealth(Math.min(input.readDouble(), player.getMaxHealth()));
                player.setAbsorptionAmount(input.readDouble());
            });
            case FOOD -> decode(payload, input -> {
                player.setFoodLevel(input.readInt());
                player.setSaturation(input.readFloat());
                player.setExhaustion(input.readFloat());
            });
            case POTION_EFFECTS -> applyPotionEffects(player, payload);
            case GAME_MODE -> decode(payload, input -> restoreGameMode(player, GameMode.valueOf(readString(input))));
            case ABILITIES -> decode(payload, input -> {
                final boolean allowFlight = input.readBoolean();
                final boolean flying = input.readBoolean();
                player.setAllowFlight(allowFlight);
                player.setFlying(allowFlight && flying);
                player.setFlySpeed(input.readFloat());
                player.setWalkSpeed(input.readFloat());
            });
            case ADVANCEMENTS -> applyAdvancements(player, payload);
            case STATISTICS -> applyStatistics(player, payload);
        }
    }

    private static byte[] capturePotionEffects(final Collection<PotionEffect> effects) throws IOException {
        return encode(output -> {
            output.writeInt(effects.size());
            for (final PotionEffect effect : effects) {
                writeString(output, effect.getType().getKey().toString());
                output.writeInt(effect.getDuration());
                output.writeInt(effect.getAmplifier());
                output.writeBoolean(effect.isAmbient());
                output.writeBoolean(effect.hasParticles());
                output.writeBoolean(effect.hasIcon());
            }
        });
    }

    private static void applyPotionEffects(final Player player, final byte[] payload) throws IOException {
        decode(payload, input -> {
            final int count = readCount(input);
            final List<PotionEffect> restored = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                final NamespacedKey key = NamespacedKey.fromString(readString(input));
                final PotionEffectType type = key == null ? null : Registry.POTION_EFFECT_TYPE.get(key);
                final int duration = input.readInt();
                final int amplifier = input.readInt();
                final boolean ambient = input.readBoolean();
                final boolean particles = input.readBoolean();
                final boolean icon = input.readBoolean();
                if (type == null) {
                    throw new ProtocolException("Unknown potion effect in player snapshot");
                }
                restored.add(new PotionEffect(type, duration, amplifier, ambient, particles, icon));
            }
            if (player instanceof final org.bukkit.craftbukkit.entity.CraftPlayer craftPlayer) {
                craftPlayer.getHandle().shardingbaseRestoreEffects(restored.stream()
                    .map(org.bukkit.craftbukkit.potion.CraftPotionUtil::fromBukkit)
                    .toList());
            } else {
                for (final PotionEffect effect : List.copyOf(player.getActivePotionEffects())) {
                    player.removePotionEffect(effect.getType());
                }
                for (final PotionEffect effect : restored) {
                    player.addPotionEffect(effect);
                }
            }
        });
    }

    private static void restoreGameMode(final Player player, final GameMode gameMode) {
        if (player instanceof final org.bukkit.craftbukkit.entity.CraftPlayer craftPlayer) {
            craftPlayer.getHandle().gameMode.shardingbaseRestoreGameMode(
                net.minecraft.world.level.GameType.byId(gameMode.getValue())
            );
        } else {
            player.setGameMode(gameMode);
        }
    }

    private static byte[] captureAdvancements(final Player player) throws IOException {
        final List<AdvancementState> states = new ArrayList<>();
        final Iterator<Advancement> advancements = Bukkit.advancementIterator();
        while (advancements.hasNext()) {
            final Advancement advancement = advancements.next();
            final AdvancementProgress progress = player.getAdvancementProgress(advancement);
            states.add(new AdvancementState(advancement.getKey().toString(), List.copyOf(progress.getAwardedCriteria())));
        }
        return encode(output -> {
            output.writeInt(states.size());
            for (final AdvancementState state : states) {
                writeString(output, state.key());
                output.writeInt(state.criteria().size());
                for (final String criterion : state.criteria()) {
                    writeString(output, criterion);
                }
            }
        });
    }

    private static void applyAdvancements(final Player player, final byte[] payload) throws IOException {
        final Map<String, Set<String>> desired = new HashMap<>();
        decode(payload, input -> {
            final int count = readCount(input);
            for (int index = 0; index < count; index++) {
                final String key = readString(input);
                final int criterionCount = readCount(input);
                final java.util.HashSet<String> criteria = new java.util.HashSet<>();
                for (int criterion = 0; criterion < criterionCount; criterion++) {
                    criteria.add(readString(input));
                }
                desired.put(key, Set.copyOf(criteria));
            }
        });
        if (player instanceof final org.bukkit.craftbukkit.entity.CraftPlayer craftPlayer) {
            final Map<net.minecraft.resources.Identifier, Set<String>> restored = new HashMap<>();
            for (final Map.Entry<String, Set<String>> entry : desired.entrySet()) {
                final net.minecraft.resources.Identifier identifier =
                    net.minecraft.resources.Identifier.tryParse(entry.getKey());
                if (identifier == null) {
                    throw new ProtocolException("Invalid advancement key in player snapshot");
                }
                restored.put(identifier, entry.getValue());
            }
            craftPlayer.getHandle().getAdvancements().shardingbaseRestoreProgress(
                craftPlayer.getHandle().level().getServer().getAdvancements(),
                restored
            );
            return;
        }
        final Iterator<Advancement> advancements = Bukkit.advancementIterator();
        while (advancements.hasNext()) {
            final Advancement advancement = advancements.next();
            final AdvancementProgress progress = player.getAdvancementProgress(advancement);
            for (final String criterion : List.copyOf(progress.getAwardedCriteria())) {
                progress.revokeCriteria(criterion);
            }
            for (final String criterion : desired.getOrDefault(advancement.getKey().toString(), Set.of())) {
                progress.awardCriteria(criterion);
            }
        }
    }

    private static byte[] captureStatistics(final Player player) throws IOException {
        final List<StatisticState> values = new ArrayList<>();
        forEachStatistic((statistic, qualifier, entityType) -> {
            final int value = statisticValue(player, statistic, qualifier, entityType);
            if (value != 0) {
                values.add(new StatisticState(
                    statistic.name(),
                    qualifier == null ? "" : qualifier.name(),
                    entityType == null ? "" : entityType.name(),
                    value
                ));
            }
        });
        return encode(output -> {
            output.writeInt(values.size());
            for (final StatisticState state : values) {
                writeString(output, state.statistic());
                writeString(output, state.material());
                writeString(output, state.entityType());
                output.writeInt(state.value());
            }
        });
    }

    private static void applyStatistics(final Player player, final byte[] payload) throws IOException {
        forEachStatistic((statistic, qualifier, entityType) -> setStatistic(player, statistic, qualifier, entityType, 0));
        decode(payload, input -> {
            final int count = readCount(input);
            for (int index = 0; index < count; index++) {
                final Statistic statistic = Statistic.valueOf(readString(input));
                final String materialName = readString(input);
                final String entityName = readString(input);
                final Material material = materialName.isEmpty() ? null : Material.valueOf(materialName);
                final EntityType entityType = entityName.isEmpty() ? null : EntityType.valueOf(entityName);
                setStatistic(player, statistic, material, entityType, input.readInt());
            }
        });
    }

    private static void forEachStatistic(final StatisticConsumer consumer) {
        for (final Statistic statistic : Statistic.values()) {
            switch (statistic.getType()) {
                case UNTYPED -> consumer.accept(statistic, null, null);
                case ITEM -> {
                    for (final Material material : Material.values()) {
                        if (material.isItem()) {
                            consumer.accept(statistic, material, null);
                        }
                    }
                }
                case BLOCK -> {
                    for (final Material material : Material.values()) {
                        if (material.isBlock()) {
                            consumer.accept(statistic, material, null);
                        }
                    }
                }
                case ENTITY -> {
                    for (final EntityType entityType : EntityType.values()) {
                        if (entityType.isAlive()) {
                            consumer.accept(statistic, null, entityType);
                        }
                    }
                }
            }
        }
    }

    private static int statisticValue(
        final Player player,
        final Statistic statistic,
        final Material material,
        final EntityType entityType
    ) {
        try {
            if (material != null) {
                return player.getStatistic(statistic, material);
            }
            if (entityType != null) {
                return player.getStatistic(statistic, entityType);
            }
            return player.getStatistic(statistic);
        } catch (IllegalArgumentException _) {
            return 0;
        }
    }

    private static void setStatistic(
        final Player player,
        final Statistic statistic,
        final Material material,
        final EntityType entityType,
        final int value
    ) {
        try {
            if (material != null) {
                player.setStatistic(statistic, material, value);
            } else if (entityType != null) {
                player.setStatistic(statistic, entityType, value);
            } else {
                player.setStatistic(statistic, value);
            }
        } catch (IllegalArgumentException _) {
        }
    }

    private static byte[] encode(final OutputWriter writer) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writer.write(output);
        }
        final byte[] payload = bytes.toByteArray();
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Portable player category exceeds the transport limit");
        }
        return payload;
    }

    private static void decode(final byte[] payload, final InputReader reader) throws IOException {
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Portable player category exceeds the transport limit");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            reader.read(input);
            if (input.available() != 0) {
                throw new ProtocolException("Trailing portable player category data");
            }
        } catch (IllegalArgumentException exception) {
            throw new ProtocolException("Invalid portable player category data", exception);
        }
    }

    private static int readCount(final DataInputStream input) throws IOException {
        final int count = input.readInt();
        if (count < 0 || count > MAX_COLLECTION_ENTRIES) {
            throw new ProtocolException("Invalid portable player collection size: " + count);
        }
        return count;
    }

    private static void writeString(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new ProtocolException("Portable player string is too large");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid portable player string length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface OutputWriter {
        void write(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface InputReader {
        void read(DataInputStream input) throws IOException;
    }

    @FunctionalInterface
    private interface StatisticConsumer {
        void accept(Statistic statistic, Material material, EntityType entityType);
    }

    private record AdvancementState(String key, List<String> criteria) {
    }

    private record StatisticState(String statistic, String material, String entityType, int value) {
    }
}
