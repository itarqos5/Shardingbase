package dev.shardingbase.server.player;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerSnapshot;
import java.util.EnumSet;
import java.util.UUID;
import org.bukkit.GameMode;
import org.bukkit.Location;
import org.bukkit.entity.Player;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PortablePlayerStateAdapterTest {
    @Test
    void roundTripsScalarCategoriesWithoutTouchingRoutingState() throws Exception {
        final UUID playerId = UUID.randomUUID();
        final Player player = Mockito.mock(Player.class);
        when(player.getUniqueId()).thenReturn(playerId);
        when(player.getExp()).thenReturn(0.75F);
        when(player.getLevel()).thenReturn(12);
        when(player.getTotalExperience()).thenReturn(450);
        when(player.getHealth()).thenReturn(17.5D);
        when(player.getMaxHealth()).thenReturn(20.0D);
        when(player.getAbsorptionAmount()).thenReturn(3.0D);
        when(player.getFoodLevel()).thenReturn(16);
        when(player.getSaturation()).thenReturn(4.5F);
        when(player.getExhaustion()).thenReturn(1.25F);
        when(player.getGameMode()).thenReturn(GameMode.ADVENTURE);
        when(player.getAllowFlight()).thenReturn(true);
        when(player.isFlying()).thenReturn(true);
        when(player.getFlySpeed()).thenReturn(0.2F);
        when(player.getWalkSpeed()).thenReturn(0.3F);
        final PortablePlayerStateAdapter adapter = new PortablePlayerStateAdapter();
        final EnumSet<PlayerDataCategory> categories = EnumSet.of(
            PlayerDataCategory.EXPERIENCE,
            PlayerDataCategory.HEALTH,
            PlayerDataCategory.FOOD,
            PlayerDataCategory.GAME_MODE,
            PlayerDataCategory.ABILITIES
        );

        final PlayerSnapshot snapshot = adapter.capture(player, 9, "backend-a", categories);
        adapter.apply(player, snapshot);

        assertEquals(categories, snapshot.categories().keySet());
        verify(player).setExp(0.75F);
        verify(player).setLevel(12);
        verify(player).setTotalExperience(450);
        verify(player).setHealth(17.5D);
        verify(player).setAbsorptionAmount(3.0D);
        verify(player).setFoodLevel(16);
        verify(player).setGameMode(GameMode.ADVENTURE);
        verify(player).setAllowFlight(true);
        verify(player).setFlying(true);
        verify(player, never()).teleport(Mockito.any(Location.class));
    }
}
