package dev.shardingbase.velocity;

import com.google.inject.Inject;
import com.velocitypowered.api.event.EventTask;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.ProxyServer;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import java.io.IOException;
import java.nio.file.Path;
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
    private final Path dataDirectory;
    private volatile ControlServer controlServer;

    @Inject
    public ShardingbaseVelocity(
        final ProxyServer proxy,
        final Logger logger,
        final @DataDirectory Path dataDirectory
    ) {
        this.proxy = proxy;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public EventTask onProxyInitialization(final ProxyInitializeEvent event) {
        return EventTask.async(this::initialize);
    }

    @Subscribe
    public void onProxyShutdown(final ProxyShutdownEvent event) {
        final ControlServer current = this.controlServer;
        if (current != null) {
            try {
                current.close();
            } catch (final IOException exception) {
                this.logger.warn("Unable to close the Shardingbase control listener cleanly", exception);
            }
        }
    }

    private void initialize() {
        try {
            final VelocityConfiguration configuration = VelocityConfiguration.load(this.dataDirectory);
            final TlsMaterial tlsMaterial = TlsMaterial.loadOrCreate(configuration);
            final BackendRegistry registry = new BackendRegistry(configuration.databasePath());
            new PlayerStateStore(configuration.databasePath());
            this.controlServer = new ControlServer(this.proxy, this.logger, configuration, tlsMaterial, registry);
            this.logger.info(
                "Shardingbase controller listening on {}:{} for {} registered Velocity backend(s); protocol {}; TLS SHA-256 {}",
                configuration.bindAddress(),
                configuration.controlPort(),
                this.proxy.getAllServers().size(),
                ShardingbaseProtocol.VERSION,
                tlsMaterial.fingerprint()
            );
        } catch (final Exception exception) {
            this.logger.error("Shardingbase controller failed to initialize; distributed features are unavailable", exception);
        }
    }
}
