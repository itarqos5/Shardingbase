package dev.shardingbase.node;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackendProcessTest {
    @Test
    void constructsAChildCommandWithJvmAndServerArgumentsInOrder() {
        final Path javaExecutable = Path.of("java-test");
        final Path backend = Path.of("server-root", "Shardingbase-backend.jar");

        final List<String> command = BackendProcess.command(
            javaExecutable,
            List.of("-Xms128M", "-Xmx4G", "-Dterminal.jline=false"),
            backend,
            new String[] {"--nogui", "--port", "25566"}
        );

        assertEquals(List.of(
            javaExecutable.toString(),
            "-Xms128M",
            "-Xmx4G",
            "-Dterminal.jline=false",
            "-jar",
            backend.toString(),
            "--nogui",
            "--port",
            "25566"
        ), command);
    }

    @Test
    void identifiesOneShotServerArguments() {
        assertTrue(ShardingbaseNode.isOneShot(new String[] {"--help"}));
        assertTrue(ShardingbaseNode.isOneShot(new String[] {"--nogui", "--version"}));
        assertFalse(ShardingbaseNode.isOneShot(new String[] {"--nogui", "--port", "25565"}));
    }

    @Test
    void givesConfiguredMemoryToBackendInsteadOfNodeHeap() throws Exception {
        final List<String> arguments = BackendProcess.backendJvmArguments(
            List.of("-Xms32M", "-Xmx128M", "-XX:MaxRAMPercentage=95", "-Dexample=true"),
            Map.of(BackendProcess.BACKEND_MEMORY_ENVIRONMENT, "4096")
        );

        assertEquals(List.of("-Dexample=true", "-Xms128M", "-Xmx4096M"), arguments);
    }

    @Test
    void rejectsUnsafeBackendMemory() {
        final IOException exception = assertThrows(IOException.class, () -> BackendProcess.backendJvmArguments(
            List.of("-Xmx128M"),
            Map.of(BackendProcess.BACKEND_MEMORY_ENVIRONMENT, "64")
        ));
        assertTrue(exception.getMessage().contains("at least 512"));
    }

    @Test
    void writesOneCanonicalConsoleCommandLine() throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        BackendProcess.writeConsoleCommand(output, "stop");

        assertEquals("stop" + System.lineSeparator(), output.toString(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> BackendProcess.writeConsoleCommand(output, "stop\nrestart"));
    }
}
