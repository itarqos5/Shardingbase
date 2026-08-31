package dev.shardingbase.node;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProxyValidationClientTest {
    @Test
    void reportsEveryMissingControllerSetting() {
        final String problem = ProxyValidationClient.configurationProblem(Map.of());

        assertTrue(problem.contains(ProxyValidationClient.CONTROLLER_URI_ENVIRONMENT_VARIABLE));
        assertTrue(problem.contains(ProxyValidationClient.CERTIFICATE_FINGERPRINT_ENVIRONMENT_VARIABLE));
        assertTrue(problem.contains(ProxyValidationClient.CREDENTIAL_ENVIRONMENT_VARIABLE));
        assertTrue(problem.contains(ProxyValidationClient.NODE_ID_ENVIRONMENT_VARIABLE));
    }

    @Test
    void rejectsMalformedControllerIdentityBeforeStartingTheConnectionLoop() {
        final Map<String, String> environment = Map.of(
            ProxyValidationClient.CONTROLLER_URI_ENVIRONMENT_VARIABLE, "http://proxy.example.test:8443",
            ProxyValidationClient.CERTIFICATE_FINGERPRINT_ENVIRONMENT_VARIABLE, "not-a-fingerprint",
            ProxyValidationClient.CREDENTIAL_ENVIRONMENT_VARIABLE, "credential",
            ProxyValidationClient.NODE_ID_ENVIRONMENT_VARIABLE, "node-a"
        );

        assertEquals("Controller URI must use tls and include an explicit port",
            ProxyValidationClient.configurationProblem(environment));
    }

    @Test
    void acceptsACompleteControllerIdentity() {
        final Map<String, String> environment = Map.of(
            ProxyValidationClient.CONTROLLER_URI_ENVIRONMENT_VARIABLE, "tls://proxy.example.test:8443",
            ProxyValidationClient.CERTIFICATE_FINGERPRINT_ENVIRONMENT_VARIABLE, "A".repeat(64),
            ProxyValidationClient.CREDENTIAL_ENVIRONMENT_VARIABLE, "credential",
            ProxyValidationClient.NODE_ID_ENVIRONMENT_VARIABLE, "node-a"
        );

        assertNull(ProxyValidationClient.configurationProblem(environment));
    }
}
