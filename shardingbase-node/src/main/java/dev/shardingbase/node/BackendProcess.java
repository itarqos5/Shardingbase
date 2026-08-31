package dev.shardingbase.node;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

/**
 * Stateful supervisor for the extracted backend JVM.
 *
 * <p>The child owns stdout and stderr. The node owns stdin so it can relay the
 * process manager's console while still being able to issue an ordered,
 * graceful Minecraft {@code stop} during an offline world transaction.</p>
 */
final class BackendProcess implements AutoCloseable {
    static final String BACKEND_MEMORY_ENVIRONMENT = "SHARDINGBASE_BACKEND_MEMORY_MB";
    static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(60);
    static final Duration NODE_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
    static final Duration TERMINATION_TIMEOUT = Duration.ofSeconds(5);
    static final String FAWE_ADAPTER_PROPERTY = "worldedit.bukkit.adapter";
    static final String PAPER_26_2_FAWE_ADAPTER =
        "com.sk89q.worldedit.bukkit.adapter.impl.fawe.v26_2.PaperweightFaweAdapter";
    private static final int MINIMUM_BACKEND_MEMORY_MIB = 512;

    private final Path backendJar;
    private final Path workingDirectory;
    private final String[] serverArguments;
    private final Map<String, String> childEnvironment;
    private final List<String> jvmArguments;
    private final InputStream consoleInput;
    private final ProcessLauncher launcher;
    private final Object inputLock = new Object();
    private final Thread consoleRelay;
    private final Thread shutdownHook;

    private Process process;
    private Integer lastExitCode;
    private boolean gracefulStopRequested;
    private boolean transactionStopRequested;
    private boolean closed;

    BackendProcess(
        final Path backendJar,
        final Path workingDirectory,
        final String[] serverArguments,
        final Map<String, String> childEnvironment,
        final List<String> inheritedJvmArguments,
        final Map<String, String> environment,
        final InputStream consoleInput,
        final ProcessLauncher launcher
    ) throws IOException {
        this.backendJar = backendJar.toAbsolutePath().normalize();
        this.workingDirectory = workingDirectory.toAbsolutePath().normalize();
        if (!Files.isDirectory(this.workingDirectory)) {
            throw new IOException("Backend working directory does not exist: " + this.workingDirectory);
        }
        this.serverArguments = serverArguments.clone();
        this.childEnvironment = Map.copyOf(childEnvironment);
        this.jvmArguments = backendJvmArguments(
            inheritedJvmArguments,
            environment,
            hasFastAsyncWorldEdit(this.workingDirectory.resolve("plugins"))
        );
        this.consoleInput = Objects.requireNonNull(consoleInput, "consoleInput");
        this.launcher = Objects.requireNonNull(launcher, "launcher");
        this.consoleRelay = Thread.ofPlatform()
            .daemon(true)
            .name("Shardingbase Backend Console")
            .unstarted(this::relayConsole);
        this.shutdownHook = Thread.ofPlatform()
            .name("shardingbase-backend-shutdown")
            .unstarted(this::stopForNodeShutdown);
    }

    static BackendProcess launch(
        final Path backendJar,
        final Path workingDirectory,
        final String[] serverArguments,
        final Map<String, String> childEnvironment
    ) throws IOException {
        final BackendProcess supervisor = new BackendProcess(
            backendJar,
            workingDirectory,
            serverArguments,
            childEnvironment,
            ManagementFactory.getRuntimeMXBean().getInputArguments(),
            System.getenv(),
            System.in,
            ProcessBuilder::start
        );
        supervisor.start();
        Runtime.getRuntime().addShutdownHook(supervisor.shutdownHook);
        supervisor.consoleRelay.start();
        return supervisor;
    }

    synchronized void start() throws IOException {
        this.requireOpen();
        if (this.process != null && this.process.isAlive()) {
            throw new IOException("Shardingbase backend is already running");
        }
        final ProcessBuilder builder = new ProcessBuilder(command(
            javaExecutable(),
            this.jvmArguments,
            this.backendJar,
            this.serverArguments
        ))
            .directory(this.workingDirectory.toFile())
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.environment().putAll(this.childEnvironment);
        this.process = this.launcher.start(builder);
        this.lastExitCode = null;
        this.gracefulStopRequested = false;
        this.transactionStopRequested = false;
        this.notifyAll();
    }

    int waitForExit() throws InterruptedException {
        while (true) {
            final Process child;
            synchronized (this) {
                child = this.process;
                if (child == null) {
                    throw new IllegalStateException("Shardingbase backend has not been started");
                }
            }
            final int exitCode = child.waitFor();
            synchronized (this) {
                this.recordExit(child, exitCode);
                if (!this.transactionStopRequested || this.closed) {
                    return exitCode;
                }
                while (!this.closed && this.process == child) {
                    this.wait();
                }
                if (this.closed) {
                    return exitCode;
                }
            }
        }
    }

    boolean stopGracefully() throws IOException, InterruptedException {
        return this.stopGracefully(SHUTDOWN_TIMEOUT);
    }

    boolean stopForTransaction() throws IOException, InterruptedException {
        synchronized (this) {
            this.requireOpen();
            this.transactionStopRequested = true;
        }
        boolean stopped = false;
        try {
            stopped = this.stopGracefully(SHUTDOWN_TIMEOUT);
            return stopped;
        } finally {
            if (!stopped) {
                synchronized (this) {
                    this.transactionStopRequested = false;
                    this.notifyAll();
                }
            }
        }
    }

    boolean stopGracefully(final Duration timeout) throws IOException, InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Graceful shutdown timeout must be positive");
        }
        final Process child;
        synchronized (this) {
            child = this.process;
            if (child == null || !child.isAlive()) {
                if (child != null) {
                    this.recordExit(child, child.exitValue());
                }
                return true;
            }
            this.gracefulStopRequested = true;
        }
        this.writeToChild(child, "stop");
        if (!child.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            return false;
        }
        this.recordExit(child, child.exitValue());
        return true;
    }

    synchronized void restart() throws IOException {
        this.requireOpen();
        if (this.process != null && this.process.isAlive()) {
            throw new IOException("Cannot restart Shardingbase while the backend is still running");
        }
        this.start();
    }

    synchronized Status status() {
        if (this.process != null && !this.process.isAlive() && this.lastExitCode == null) {
            this.recordExit(this.process, this.process.exitValue());
        }
        return new Status(
            this.process != null && this.process.isAlive(),
            this.process == null ? null : this.process.pid(),
            this.lastExitCode,
            this.gracefulStopRequested
        );
    }

    private void relayConsole() {
        final byte[] buffer = new byte[1024];
        try {
            int read;
            while ((read = this.consoleInput.read(buffer)) >= 0) {
                if (read == 0) {
                    continue;
                }
                final Process child;
                synchronized (this) {
                    child = this.process;
                }
                if (child != null && child.isAlive()) {
                    synchronized (this.inputLock) {
                        final OutputStream childInput = child.getOutputStream();
                        childInput.write(buffer, 0, read);
                        childInput.flush();
                    }
                }
            }
        } catch (final IOException exception) {
            synchronized (this) {
                if (!this.closed) {
                    System.err.println("Shardingbase backend console relay stopped: " + exception.getMessage());
                }
            }
        }
    }

    private void writeToChild(final Process child, final String command) throws IOException {
        synchronized (this.inputLock) {
            writeConsoleCommand(child.getOutputStream(), command);
        }
    }

    private void stopForNodeShutdown() {
        boolean stopped = false;
        try {
            stopped = this.stopGracefully(NODE_SHUTDOWN_TIMEOUT);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (final IOException exception) {
            System.err.println("Unable to send the graceful backend stop command: " + exception.getMessage());
        }
        if (!stopped) {
            this.terminateBackend("Backend did not stop within 30 seconds during node shutdown");
        }
    }

    private void terminateBackend(final String reason) {
        final Process child;
        synchronized (this) {
            child = this.process;
        }
        if (child == null || !child.isAlive()) {
            return;
        }
        System.err.println(reason + "; terminating the backend process.");
        child.destroy();
        try {
            if (!child.waitFor(TERMINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                System.err.println("Backend ignored termination; forcing the process to exit.");
                child.destroyForcibly();
                child.waitFor(TERMINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            child.destroyForcibly();
        } finally {
            if (!child.isAlive()) {
                this.recordExit(child, child.exitValue());
            }
        }
    }

    private synchronized void recordExit(final Process expected, final int exitCode) {
        if (this.process == expected) {
            this.lastExitCode = exitCode;
        }
    }

    private synchronized void requireOpen() throws IOException {
        if (this.closed) {
            throw new IOException("Shardingbase backend supervisor is closed");
        }
    }

    static void writeConsoleCommand(final OutputStream output, final String command) throws IOException {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(command, "command");
        if (command.isBlank() || command.indexOf('\r') >= 0 || command.indexOf('\n') >= 0) {
            throw new IOException("Backend console command must be one non-blank line");
        }
        output.write((command + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    static List<String> backendJvmArguments(
        final List<String> inheritedArguments,
        final Map<String, String> environment
    ) throws IOException {
        return backendJvmArguments(inheritedArguments, environment, false);
    }

    static List<String> backendJvmArguments(
        final List<String> inheritedArguments,
        final Map<String, String> environment,
        final boolean fastAsyncWorldEditInstalled
    ) throws IOException {
        final String configuredMemory = environment.get(BACKEND_MEMORY_ENVIRONMENT);
        final List<String> childArguments = new ArrayList<>(inheritedArguments.size() + 3);
        if (configuredMemory == null || configuredMemory.isBlank()) {
            childArguments.addAll(inheritedArguments);
        } else {
            final int memoryMib;
            try {
                memoryMib = Integer.parseInt(configuredMemory.trim());
            } catch (NumberFormatException exception) {
                throw new IOException(BACKEND_MEMORY_ENVIRONMENT + " must be an integer number of MiB", exception);
            }
            if (memoryMib < MINIMUM_BACKEND_MEMORY_MIB) {
                throw new IOException(BACKEND_MEMORY_ENVIRONMENT + " must be at least " + MINIMUM_BACKEND_MEMORY_MIB);
            }
            for (final String argument : inheritedArguments) {
                if (!isHeapSizingArgument(argument)) {
                    childArguments.add(argument);
                }
            }
            childArguments.add("-Xms128M");
            childArguments.add("-Xmx" + memoryMib + "M");
        }
        if (fastAsyncWorldEditInstalled
            && childArguments.stream().noneMatch(BackendProcess::isFaweAdapterArgument)) {
            childArguments.add("-D" + FAWE_ADAPTER_PROPERTY + '=' + PAPER_26_2_FAWE_ADAPTER);
        }
        return List.copyOf(childArguments);
    }

    static boolean hasFastAsyncWorldEdit(final Path pluginsDirectory) throws IOException {
        if (!Files.isDirectory(pluginsDirectory)) {
            return false;
        }
        try (var entries = Files.list(pluginsDirectory)) {
            for (final Path entry : entries.filter(Files::isRegularFile).toList()) {
                if (!entry.getFileName().toString().toLowerCase(java.util.Locale.ROOT).endsWith(".jar")) {
                    continue;
                }
                try (JarFile plugin = new JarFile(entry.toFile())) {
                    if (plugin.getJarEntry("com/fastasyncworldedit/core/Fawe.class") != null) {
                        return true;
                    }
                } catch (IOException _) {
                    // Paper owns plugin validation and will report an invalid JAR with its normal diagnostics.
                }
            }
        }
        return false;
    }

    private static boolean isFaweAdapterArgument(final String argument) {
        return argument.startsWith("-D" + FAWE_ADAPTER_PROPERTY + '=');
    }

    private static boolean isHeapSizingArgument(final String argument) {
        return argument.startsWith("-Xms")
            || argument.startsWith("-Xmx")
            || argument.startsWith("-XX:MaxRAMPercentage=")
            || argument.startsWith("-XX:InitialRAMPercentage=")
            || argument.startsWith("-XX:MinRAMPercentage=");
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

    @Override
    public void close() {
        synchronized (this) {
            if (this.closed) {
                return;
            }
            this.closed = true;
            this.notifyAll();
        }
        try {
            if (!this.stopGracefully()) {
                this.terminateBackend("Backend did not stop within 60 seconds while closing the supervisor");
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            this.terminateBackend("Interrupted while waiting for the backend to stop");
        } catch (final IOException exception) {
            System.err.println("Unable to stop the Shardingbase backend: " + exception.getMessage());
            this.terminateBackend("Graceful backend shutdown failed");
        }
        this.consoleRelay.interrupt();
        try {
            Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
        } catch (IllegalStateException _) {
            // The JVM is already shutting down and the hook owns child cleanup.
        }
    }

    record Status(boolean running, Long processId, Integer lastExitCode, boolean gracefulStopRequested) {
    }

    @FunctionalInterface
    interface ProcessLauncher {
        Process start(ProcessBuilder builder) throws IOException;
    }
}
