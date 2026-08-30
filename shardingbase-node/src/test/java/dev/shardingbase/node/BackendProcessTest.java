package dev.shardingbase.node;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
