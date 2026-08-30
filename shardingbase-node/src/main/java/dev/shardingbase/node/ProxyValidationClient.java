package dev.shardingbase.node;

import dev.shardingbase.protocol.FrameCodec;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.protocol.ValidationPayloadCodec;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationRequest;
import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/** Opens a pinned TLS control connection to the Velocity authority. */
final class ProxyValidationClient {
    static final String CONTROLLER_URI_ENVIRONMENT_VARIABLE = "SHARDINGBASE_CONTROLLER_URI";
    static final String CERTIFICATE_FINGERPRINT_ENVIRONMENT_VARIABLE = "SHARDINGBASE_CERTIFICATE_SHA256";
    static final String CREDENTIAL_ENVIRONMENT_VARIABLE = "SHARDINGBASE_NODE_CREDENTIAL";
    static final String NODE_ID_ENVIRONMENT_VARIABLE = "SHARDINGBASE_NODE_ID";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final Map<String, String> environment;

    ProxyValidationClient() {
        this(System.getenv());
    }

    ProxyValidationClient(final Map<String, String> environment) {
        this.environment = Map.copyOf(environment);
    }

    ValidationResponse validate(
        final String serverId,
        final String serverName,
        final String minecraftVersion,
        final String shardingbaseVersion
    ) throws IOException {
        final String controller = this.environment.get(CONTROLLER_URI_ENVIRONMENT_VARIABLE);
        final String fingerprint = normalizeFingerprint(
            this.environment.get(CERTIFICATE_FINGERPRINT_ENVIRONMENT_VARIABLE)
        );
        final String credential = this.environment.get(CREDENTIAL_ENVIRONMENT_VARIABLE);
        if (controller == null || credential == null || credential.isBlank() || fingerprint == null) {
            return new ValidationResponse(false, "node controller URI, credential, and certificate fingerprint are required", "", "");
        }

        final URI uri;
        try {
            uri = URI.create(controller);
        } catch (final IllegalArgumentException exception) {
            throw new IOException("Invalid Shardingbase controller URI", exception);
        }
        if (!("tls".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())
            || "wss".equalsIgnoreCase(uri.getScheme())) || uri.getHost() == null || uri.getPort() < 1) {
            throw new IOException("Controller URI must use tls, https, or wss and include an explicit port");
        }

        final int timeoutMillis = Math.toIntExact(TIMEOUT.toMillis());
        try (SSLSocket socket = (SSLSocket) sslContext().getSocketFactory().createSocket()) {
            socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            socket.startHandshake();
            verifyFingerprint(socket, fingerprint);

            final String nodeId = this.environment.getOrDefault(NODE_ID_ENVIRONMENT_VARIABLE, "node");
            final UUID correlationId = UUID.randomUUID();
            final ValidationRequest request = new ValidationRequest(
                credential,
                serverId,
                serverName,
                minecraftVersion,
                shardingbaseVersion
            );
            FrameCodec.write(socket.getOutputStream(), new ProtocolFrame(
                ShardingbaseProtocol.VERSION,
                ProtocolChannel.CONTROL,
                MessageType.VALIDATE_BACKEND_REQUEST,
                correlationId,
                nodeId,
                "velocity",
                ValidationPayloadCodec.encodeRequest(request)
            ));
            final ProtocolFrame response = FrameCodec.read(socket.getInputStream());
            if (!correlationId.equals(response.correlationId())
                || response.messageType() != MessageType.VALIDATE_BACKEND_RESPONSE) {
                throw new IOException("Velocity returned an unrelated validation response");
            }
            return ValidationPayloadCodec.decodeResponse(response.payload());
        }
    }

    private static SSLContext sslContext() throws IOException {
        try {
            final SSLContext context = SSLContext.getInstance("TLSv1.3");
            context.init(null, new TrustManager[] {new PinningTrustManager()}, new SecureRandom());
            return context;
        } catch (final GeneralSecurityException exception) {
            throw new IOException("Unable to initialize TLS", exception);
        }
    }

    private static void verifyFingerprint(final SSLSocket socket, final String expected) throws IOException {
        try {
            final X509Certificate certificate = (X509Certificate) socket.getSession().getPeerCertificates()[0];
            final String actual = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded())
            ).toUpperCase(Locale.ROOT);
            if (!MessageDigest.isEqual(actual.getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                expected.getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
                throw new IOException("Velocity TLS certificate fingerprint mismatch; received " + actual);
            }
        } catch (final GeneralSecurityException exception) {
            throw new IOException("Unable to verify Velocity TLS certificate", exception);
        }
    }

    private static String normalizeFingerprint(final String fingerprint) {
        if (fingerprint == null) {
            return null;
        }
        final String normalized = fingerprint.replace(":", "").replace(" ", "").toUpperCase(Locale.ROOT);
        return normalized.matches("[0-9A-F]{64}") ? normalized : null;
    }

    private static final class PinningTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authenticationType) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authenticationType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
