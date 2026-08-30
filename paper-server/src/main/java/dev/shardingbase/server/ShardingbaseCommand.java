package dev.shardingbase.server;

import dev.shardingbase.api.ShardingbaseService;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;
import org.bukkit.Bukkit;
import org.bukkit.command.CommandSender;
import org.bukkit.command.defaults.BukkitCommand;
import org.bukkit.entity.Player;
import org.bukkit.util.StringUtil;
import org.jetbrains.annotations.NotNull;

/** Built-in command for Shardingbase status, reload, and menus. */
public final class ShardingbaseCommand extends BukkitCommand {
    private static final String ADMIN_PERMISSION = "shardingbase.admin";
    private static final String RELOAD_PERMISSION = "shardingbase.reload";
    private static final String SYNC_PERMISSION = "shardingbase.sync";
    private final ShardingbaseRuntime runtime;
    private final Executor serverExecutor;

    public ShardingbaseCommand(final ShardingbaseRuntime runtime, final Executor serverExecutor) {
        super("shardingbase");
        this.runtime = runtime;
        this.serverExecutor = serverExecutor;
        this.description = "Shows and manages Shardingbase distributed features";
        this.usageMessage = "/shardingbase [reload|sync|help]";
    }

    @Override
    public boolean execute(
        final @NotNull CommandSender sender,
        final @NotNull String currentAlias,
        final @NotNull String[] args
    ) {
        if (args.length == 0) {
            if (!allowed(sender, ADMIN_PERMISSION)) {
                return true;
            }
            this.sendStatus(sender);
            return true;
        }

        return switch (args[0].toLowerCase(Locale.ROOT)) {
            case "reload" -> this.reload(sender);
            case "sync" -> this.sync(sender);
            case "help" -> this.help(sender);
            default -> {
                sender.sendMessage("Unknown subcommand. Use /shardingbase help.");
                yield true;
            }
        };
    }

    private void sendStatus(final CommandSender sender) {
        final var identity = this.runtime.identity();
        sender.sendMessage("Shardingbase " + Bukkit.getVersion());
        sender.sendMessage("Server: " + identity.serverName() + " (" + identity.serverId() + ')');
        sender.sendMessage("Feature state: " + this.runtime.featureState());
        sender.sendMessage("Proxy/peer: " + this.runtime.statusDetail());
    }

    private boolean reload(final CommandSender sender) {
        if (!allowed(sender, RELOAD_PERMISSION)) {
            return true;
        }
        sender.sendMessage("Reloading Shardingbase configuration...");
        this.runtime.reload().whenComplete((result, failure) -> this.serverExecutor.execute(() -> {
            if (failure != null) {
                sender.sendMessage("Shardingbase reload failed: " + failure.getMessage());
            } else {
                sender.sendMessage((result.successful() ? "Shardingbase reload complete: " : "Shardingbase reload rejected: ")
                    + result.message());
            }
        }));
        return true;
    }

    private boolean sync(final CommandSender sender) {
        if (!allowed(sender, SYNC_PERMISSION)) {
            return true;
        }
        if (!(sender instanceof Player)) {
            sender.sendMessage("/shardingbase sync can only be used by a player.");
            return true;
        }
        sender.sendMessage("The Shardingbase sync menu is unavailable until distributed features are enabled.");
        return true;
    }

    private boolean help(final CommandSender sender) {
        if (!allowed(sender, ADMIN_PERMISSION)) {
            return true;
        }
        sender.sendMessage("/shardingbase - show backend, proxy, and peer status");
        sender.sendMessage("/shardingbase reload - reload only Shardingbase configuration");
        sender.sendMessage("/shardingbase sync - open the player synchronization menu");
        return true;
    }

    private static boolean allowed(final CommandSender sender, final String permission) {
        if (sender.hasPermission(permission)) {
            return true;
        }
        sender.sendMessage("You do not have permission to use this command.");
        return false;
    }

    @Override
    public @NotNull List<String> tabComplete(
        final @NotNull CommandSender sender,
        final @NotNull String alias,
        final @NotNull String[] args
    ) {
        if (args.length != 1) {
            return List.of();
        }
        final List<String> available = new ArrayList<>();
        if (sender.hasPermission(RELOAD_PERMISSION)) {
            available.add("reload");
        }
        if (sender.hasPermission(SYNC_PERMISSION)) {
            available.add("sync");
        }
        if (sender.hasPermission(ADMIN_PERMISSION)) {
            available.add("help");
        }
        return StringUtil.copyPartialMatches(args[0], available, new ArrayList<>());
    }
}
