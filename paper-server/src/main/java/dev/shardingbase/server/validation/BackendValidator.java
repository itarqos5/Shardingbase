package dev.shardingbase.server.validation;

import dev.shardingbase.api.ServerIdentity;
import java.io.IOException;
/** Blocking validation boundary. Callers must invoke it on a bounded I/O executor. */
@FunctionalInterface
public interface BackendValidator {
    /**
     * Validates a backend through its local node.
     *
     * @param identity candidate identity
     * @return validation result
     * @throws IOException when the local control connection fails
     */
    ValidationResult validate(ServerIdentity identity) throws IOException;
}
