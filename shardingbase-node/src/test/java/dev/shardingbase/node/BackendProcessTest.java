package dev.shardingbase.node;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackendProcessTest {
    @Test
    void constructsAChildCommandWithJvmAndServerArgumentsInOrder() {
        final Path javaExecutable = Path.of("java-test");
        final Path backend = Path.of("server-root", "cache", "backend.jar");

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
    void givesConfiguredMemoryToBackendInsteadOfNodeHeap() throws Exception {
        final List<String> arguments = BackendProcess.backendJvmArguments(
            List.of("-Xms32M", "-Xmx128M", "-XX:MaxRAMPercentage=95", "-Dexample=true"),
            Map.of(BackendProcess.BACKEND_MEMORY_ENVIRONMENT, "4096")
        );

        assertEquals(List.of(
            "-Dexample=true",
            "-Xms128M",
            "-Xmx4096M"
        ), arguments);
    }

    @Test
    void selectsThePaper262FaweAdapterWithoutOverridingAnOperatorChoice() throws Exception {
        assertEquals(List.of(
            "-Dexample=true",
            "-Dworldedit.bukkit.adapter=" + BackendProcess.PAPER_26_2_FAWE_ADAPTER
        ), BackendProcess.backendJvmArguments(List.of("-Dexample=true"), Map.of(), true));

        assertEquals(List.of("-Dworldedit.bukkit.adapter=example.CustomAdapter"),
            BackendProcess.backendJvmArguments(
                List.of("-Dworldedit.bukkit.adapter=example.CustomAdapter"),
                Map.of(),
                true
            ));
    }

    @Test
    void detectsFaweByJarContentEvenAfterRename(@TempDir final Path directory) throws Exception {
        final Path plugins = directory.resolve("plugins");
        Files.createDirectories(plugins);
        try (JarOutputStream output = new JarOutputStream(
            Files.newOutputStream(plugins.resolve("renamed-plugin.jar")))) {
            output.putNextEntry(new JarEntry("com/fastasyncworldedit/core/Fawe.class"));
            output.write(new byte[] {0});
            output.closeEntry();
        }

        assertTrue(BackendProcess.hasFastAsyncWorldEdit(plugins));
        assertFalse(BackendProcess.hasFastAsyncWorldEdit(directory.resolve("missing")));
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

    @Test
    void waitForExitSurvivesAnOfflineTransactionRestart(@TempDir final Path directory) throws Exception {
        final TestProcess first = new TestProcess(41L);
        final TestProcess second = new TestProcess(42L);
        final ArrayDeque<TestProcess> launches = new ArrayDeque<>(List.of(first, second));
        final BackendProcess backend = new BackendProcess(
            directory.resolve("cache/backend.jar"),
            directory,
            new String[] {"nogui"},
            Map.of(),
            List.of(),
            Map.of(),
            InputStream.nullInputStream(),
            builder -> launches.removeFirst()
        );
        try {
            backend.start();
            final CompletableFuture<Integer> finalExit = CompletableFuture.supplyAsync(() -> {
                try {
                    return backend.waitForExit();
                } catch (InterruptedException exception) {
                    throw new AssertionError(exception);
                }
            });

            assertTrue(backend.stopForTransaction());
            assertFalse(finalExit.isDone());
            backend.restart();
            second.finish(17);

            assertEquals(17, finalExit.get(2, TimeUnit.SECONDS));
        } finally {
            backend.close();
        }
    }

    private static final class TestProcess extends Process {
        private final long pid;
        private final OutputStream input = new OutputStream() {
            private final ByteArrayOutputStream command = new ByteArrayOutputStream();

            @Override
            public void write(final int value) {
                if (value == '\n') {
                    if (this.command.toString(StandardCharsets.UTF_8).trim().equals("stop")) {
                        TestProcess.this.finish(0);
                    }
                    this.command.reset();
                } else if (value != '\r') {
                    this.command.write(value);
                }
            }
        };
        private boolean alive = true;
        private int exitCode;

        private TestProcess(final long pid) {
            this.pid = pid;
        }

        synchronized void finish(final int code) {
            this.exitCode = code;
            this.alive = false;
            this.notifyAll();
        }

        @Override
        public OutputStream getOutputStream() {
            return this.input;
        }

        @Override
        public InputStream getInputStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream getErrorStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public synchronized int waitFor() throws InterruptedException {
            while (this.alive) {
                this.wait();
            }
            return this.exitCode;
        }

        @Override
        public synchronized boolean waitFor(final long timeout, final TimeUnit unit) throws InterruptedException {
            if (this.alive) {
                unit.timedWait(this, timeout);
            }
            return !this.alive;
        }

        @Override
        public synchronized int exitValue() {
            if (this.alive) {
                throw new IllegalThreadStateException();
            }
            return this.exitCode;
        }

        @Override
        public void destroy() {
            this.finish(143);
        }

        @Override
        public Process destroyForcibly() {
            this.finish(137);
            return this;
        }

        @Override
        public synchronized boolean isAlive() {
            return this.alive;
        }

        @Override
        public long pid() {
            return this.pid;
        }
    }
}
