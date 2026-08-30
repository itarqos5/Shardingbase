package dev.shardingbase.server.player;

import java.lang.reflect.Proxy;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PlayerStateDestinationTest {
    @Test
    void findsNearestSafeDestinationWithinBoundedRadius() {
        final World world = worldWithOnlySafeColumn(11, 64, 10);
        final Location safe = PlayerStateCoordinator.safeDestination(
            new Location(world, 10.25, 64.0, 10.75, 92.0F, -4.0F),
            4
        );

        assertNotNull(safe);
        assertEquals(11.5, safe.getX());
        assertEquals(64.0, safe.getY());
        assertEquals(10.5, safe.getZ());
        assertEquals(92.0F, safe.getYaw());
        assertEquals(-4.0F, safe.getPitch());
    }

    private static World worldWithOnlySafeColumn(final int safeX, final int safeY, final int safeZ) {
        return (World) Proxy.newProxyInstance(
            World.class.getClassLoader(),
            new Class<?>[] {World.class},
            (proxy, method, arguments) -> switch (method.getName()) {
                case "getMinHeight" -> -64;
                case "getMaxHeight" -> 320;
                case "getBlockAt" -> {
                    final int x;
                    final int y;
                    final int z;
                    if (arguments.length == 1) {
                        final Location location = (Location) arguments[0];
                        x = location.getBlockX();
                        y = location.getBlockY();
                        z = location.getBlockZ();
                    } else {
                        x = (int) arguments[0];
                        y = (int) arguments[1];
                        z = (int) arguments[2];
                    }
                    yield block(
                        x == safeX && z == safeZ && (y == safeY || y == safeY + 1),
                        x == safeX && z == safeZ && y == safeY - 1
                    );
                }
                default -> defaultValue(method.getReturnType());
            }
        );
    }

    private static Block block(final boolean passable, final boolean solid) {
        return (Block) Proxy.newProxyInstance(
            Block.class.getClassLoader(),
            new Class<?>[] {Block.class},
            (proxy, method, arguments) -> switch (method.getName()) {
                case "isPassable" -> passable;
                case "isSolid" -> solid;
                default -> defaultValue(method.getReturnType());
            }
        );
    }

    private static Object defaultValue(final Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }
}
