package dev.shardingbase.node;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Launches and supervises the extracted backend in its own JVM.
 */
final class BackendProcess {
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60L;

    private BackendProcess() {
    }

    static int run(
        final Path backendJar,
        final String[] serverArguments,
        final Map<String, String> childEnvironment
    ) throws IOException, InterruptedException {
        final Path normalizedBackend = backendJar.toAbsolutePath().normalize();
        final Path workingDirectory = normalizedBackend.getParent();
        if (workingDirectory == null) {
            throw new IOException("Backend JAR has no working directory: " + normalizedBackend);
        }

        final ProcessBuilder builder = new ProcessBuilder(command(
            javaExecutable(),
            ManagementFactory.getRuntimeMXBean().getInputArguments(),
            normalizedBackend,
            serverArguments
        ))
            .directory(workingDirectory.toFile())
            .inheritIO();
        builder.environment().putAll(childEnvironment);
        final Process process = builder.start();

        final Thread shutdownHook = Thread.ofPlatform()
            .name("shardingbase-backend-shutdown")
            .unstarted(() -> stop(process));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            return process.waitFor();
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException _) {
                // The JVM is already shutting down and the hook is responsible for the child.
            }
        }
    }

    static List<String> command(
        final Path javaExecutable,
        final List<String> jvmArguments,
        final Path backendJar,
        final String[] serverArguments
    ) {
        final List<String> command = new ArrayList<>(jvmArguments.size() + serverArguments.length + 3);
        command.add(javaExecutable.toString());
        command.addAll(jvmArguments);
        command.add("-jar");
        command.add(backendJar.toString());
        command.addAll(Arrays.asList(serverArguments));
        return List.copyOf(command);
    }

    private static Path javaExecutable() {
        final String executable = System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", executable).toAbsolutePath().normalize();
    }

    private static void stop(final Process process) {
        if (!process.isAlive()) {
            return;
        }

        process.destroy();
        try {
            if (!process.waitFor(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                System.err.println("Backend did not stop within 60 seconds; refusing to force-kill it.");
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }
}
