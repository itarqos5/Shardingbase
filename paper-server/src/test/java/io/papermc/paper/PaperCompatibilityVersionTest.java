package io.papermc.paper;

import java.util.jar.Attributes;
import java.util.jar.Manifest;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Normal
public class PaperCompatibilityVersionTest {
    @Test
    public void formatsExactPaperBaseline() {
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Paper-Compatibility-Build", "121");
        manifest.getMainAttributes().putValue(
            "Paper-Compatibility-Commit",
            "a2a42c5b12249aaba42a347327fd930a1f94af06"
        );

        assertEquals(
            "26.2-121-a2a42c5",
            PaperCompatibilityVersion.fromManifest(manifest, "26.2", "26.2-DEV-local")
        );
        assertEquals(121, PaperCompatibilityVersion.buildNumberFromManifest(manifest).orElseThrow());
    }

    @Test
    public void fallsBackWhenManifestIsUnavailableOrInvalid() {
        assertEquals(
            "26.2-DEV-local",
            PaperCompatibilityVersion.fromManifest(null, "26.2", "26.2-DEV-local")
        );
        assertTrue(PaperCompatibilityVersion.buildNumberFromManifest(null).isEmpty());

        final Manifest manifest = new Manifest();
        final Attributes attributes = manifest.getMainAttributes();
        attributes.putValue("Paper-Compatibility-Build", "latest");
        attributes.putValue("Paper-Compatibility-Commit", "not-a-commit");
        assertEquals(
            "26.2-DEV-local",
            PaperCompatibilityVersion.fromManifest(manifest, "26.2", "26.2-DEV-local")
        );
        assertTrue(PaperCompatibilityVersion.buildNumberFromManifest(manifest).isEmpty());
    }
}
