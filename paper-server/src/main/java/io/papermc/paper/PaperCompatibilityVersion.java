package io.papermc.paper;

import com.google.common.base.Strings;
import io.papermc.paper.util.JarManifests;
import java.util.Optional;
import java.util.jar.Manifest;
import java.util.regex.Pattern;
import org.bukkit.craftbukkit.CraftServer;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Supplies the exact upstream Paper build identity exposed through legacy
 * Bukkit version APIs.
 *
 * <p>The Shardingbase build and display identity remain available through
 * {@link ServerBuildInfo}. This compatibility identity deliberately points at
 * the Paper release used as Shardingbase's source baseline, rather than
 * presenting Shardingbase commits as Paper commits.</p>
 */
@ApiStatus.Internal
public final class PaperCompatibilityVersion {
    private static final String ATTRIBUTE_BUILD = "Paper-Compatibility-Build";
    private static final String ATTRIBUTE_COMMIT = "Paper-Compatibility-Commit";
    private static final Pattern BUILD = Pattern.compile("[1-9][0-9]*");
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{7,40}");

    private PaperCompatibilityVersion() {
    }

    /**
     * Returns the Paper-formatted version corresponding to this fork's exact
     * upstream baseline.
     *
     * @return a Paper-compatible simple version string
     */
    public static @NotNull String current() {
        return fromManifest(
            JarManifests.manifest(CraftServer.class),
            ServerBuildInfo.buildInfo().minecraftVersionId(),
            ServerBuildInfo.buildInfo().asString(ServerBuildInfo.StringRepresentation.VERSION_SIMPLE)
        );
    }

    static @NotNull String fromManifest(
        final Manifest manifest,
        final String minecraftVersion,
        final String fallback
    ) {
        final Optional<String> build = attribute(manifest, ATTRIBUTE_BUILD).filter(BUILD.asMatchPredicate());
        final Optional<String> commit = attribute(manifest, ATTRIBUTE_COMMIT).filter(COMMIT.asMatchPredicate());
        if (build.isEmpty() || commit.isEmpty()) {
            return fallback;
        }
        return minecraftVersion + '-' + build.get() + '-' + commit.get().substring(0, 7);
    }

    private static Optional<String> attribute(final Manifest manifest, final String name) {
        final String value = manifest == null ? null : manifest.getMainAttributes().getValue(name);
        return Optional.ofNullable(Strings.emptyToNull(value));
    }
}
