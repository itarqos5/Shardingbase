package dev.shardingbase.velocity;

import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.proxy.ProxyServer;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import org.slf4j.Logger;

@Plugin(
    id = "shardingbase",
    name = "Shardingbase",
    version = "0.1.0-SNAPSHOT",
    description = "Velocity controller for Shardingbase backends"
)
public final class ShardingbaseVelocity {
    private final ProxyServer proxy;
    private final Logger logger;

    @Inject
    public ShardingbaseVelocity(final ProxyServer proxy, final Logger logger) {
        this.proxy = proxy;
        this.logger = logger;
    }

    @Subscribe
    public void onProxyInitialization(final ProxyInitializeEvent event) {
        this.logger.info(
            "Shardingbase controller initialized for {} registered backend(s), protocol {}",
            this.proxy.getAllServers().size(),
            ShardingbaseProtocol.VERSION
        );
    }
}
