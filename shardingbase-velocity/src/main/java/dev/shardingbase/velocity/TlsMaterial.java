package dev.shardingbase.velocity;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.util.HexFormat;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/** Creates and loads the controller's TLS key material. */
final class TlsMaterial {
    private final SSLContext context;
    private final String fingerprint;

    private TlsMaterial(final SSLContext context, final String fingerprint) {
        this.context = context;
        this.fingerprint = fingerprint;
    }

    static TlsMaterial loadOrCreate(final VelocityConfiguration configuration) throws IOException {
        if (Files.notExists(configuration.keyStorePath())) {
            generate(configuration.keyStorePath(), configuration.keyStorePassword());
        }
        final char[] password = configuration.keyStorePassword().toCharArray();
        try {
            final KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (InputStream input = Files.newInputStream(configuration.keyStorePath())) {
                keyStore.load(input, password);
            }
            final KeyManagerFactory managers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            managers.init(keyStore, password);
            final SSLContext context = SSLContext.getInstance("TLSv1.3");
            context.init(managers.getKeyManagers(), null, null);
            final Certificate certificate = keyStore.getCertificate("shardingbase");
            final String fingerprint = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded())
            ).toUpperCase(Locale.ROOT);
            return new TlsMaterial(context, fingerprint);
        } catch (final GeneralSecurityException exception) {
            throw new IOException("Unable to load the Shardingbase TLS keystore", exception);
        }
    }

    SSLContext context() {
        return this.context;
    }

    String fingerprint() {
        return this.fingerprint;
    }

    private static void generate(final Path keyStore, final String password) throws IOException {
        Files.createDirectories(keyStore.getParent());
        final boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
        final Path keytool = Path.of(System.getProperty("java.home"), "bin", windows ? "keytool.exe" : "keytool");
        final Process process = new ProcessBuilder(
            keytool.toString(),
            "-genkeypair",
            "-alias", "shardingbase",
            "-keyalg", "EC",
            "-groupname", "secp256r1",
            "-validity", "3650",
            "-dname", "CN=Shardingbase",
            "-storetype", "PKCS12",
            "-keystore", keyStore.toString(),
            "-storepass", password,
            "-keypass", password,
            "-ext", "SAN=dns:localhost,ip:127.0.0.1"
        ).redirectErrorStream(true).start();
        try {
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
                throw new IOException("keytool timed out while generating the Shardingbase certificate");
            }
        } catch (final InterruptedException exception) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while generating the Shardingbase certificate", exception);
        }
        final byte[] output = process.getInputStream().readAllBytes();
        if (process.exitValue() != 0) {
            throw new IOException("keytool failed: " + new String(output, java.nio.charset.StandardCharsets.UTF_8));
        }
    }
}
