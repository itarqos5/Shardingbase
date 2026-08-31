package dev.shardingbase.node.world;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Exact file-set and SHA-256 manifest for one relayed shard tree. */
public final class TransferTreeManifest {
    public static final String FILE_NAME = ".shardingbase-transfer.sha256";

    private TransferTreeManifest() {
    }

    public static Summary write(final Path treeRoot) throws IOException {
        final Path root = requireRoot(treeRoot);
        final List<Path> files = files(root);
        final StringBuilder content = new StringBuilder();
        long bytes = 0L;
        for (final Path file : files) {
            final long size = Files.size(file);
            bytes = Math.addExact(bytes, size);
            content.append(HexFormat.of().formatHex(sha256(file)))
                .append('\t').append(size).append('\t')
                .append(relative(root, file)).append('\n');
        }
        final Path target = root.resolve(FILE_NAME);
        final Path temporary = Files.createTempFile(root, ".shardingbase-transfer-", ".tmp");
        try {
            Files.writeString(temporary, content.toString(), StandardCharsets.UTF_8);
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (final AtomicMoveNotSupportedException exception) {
                throw new IOException("Atomic transfer manifest publication is not supported", exception);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
        return new Summary(files.size(), bytes);
    }

    public static Summary verify(final Path treeRoot) throws IOException {
        return verify(treeRoot, Set.of());
    }

    static Summary verify(final Path treeRoot, final Set<String> allowedAdditionalFiles) throws IOException {
        final Path root = requireRoot(treeRoot);
        final Path manifest = root.resolve(FILE_NAME);
        if (!Files.isRegularFile(manifest)) {
            throw new IOException("Relayed shard tree has no transfer manifest");
        }
        final Map<String, Entry> expected = new LinkedHashMap<>();
        for (final String line : Files.readAllLines(manifest, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            final String[] fields = line.split("\t", 3);
            if (fields.length != 3 || !fields[0].matches("[0-9a-f]{64}") || expected.containsKey(fields[2])) {
                throw new IOException("Invalid or duplicate transfer manifest entry");
            }
            final long size;
            try {
                size = Long.parseLong(fields[1]);
            } catch (final NumberFormatException exception) {
                throw new IOException("Invalid transfer manifest file size", exception);
            }
            if (size < 0 || !safeRelative(fields[2])) {
                throw new IOException("Unsafe transfer manifest entry");
            }
            expected.put(fields[2], new Entry(size, HexFormat.of().parseHex(fields[0])));
        }
        final List<Path> actualFiles = files(root);
        long bytes = 0L;
        int files = 0;
        for (final Path file : actualFiles) {
            final String relative = relative(root, file);
            if (allowedAdditionalFiles.contains(relative)) {
                continue;
            }
            final Entry entry = expected.remove(relative);
            if (entry == null || Files.size(file) != entry.size()
                || !MessageDigest.isEqual(entry.sha256(), sha256(file))) {
                throw new IOException("Relayed shard file failed manifest verification: " + relative);
            }
            bytes = Math.addExact(bytes, entry.size());
            files++;
        }
        if (!expected.isEmpty()) {
            throw new IOException("Relayed shard manifest references missing files");
        }
        return new Summary(files, bytes);
    }

    public static List<Path> filesForTransfer(final Path treeRoot) throws IOException {
        final Path root = requireRoot(treeRoot);
        final List<Path> files = new ArrayList<>(files(root));
        final Path manifest = root.resolve(FILE_NAME);
        if (!Files.isRegularFile(manifest)) {
            throw new IOException("Transfer manifest must be written before relay");
        }
        files.add(manifest);
        return List.copyOf(files);
    }

    public static byte[] sha256(final Path file) throws IOException {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(file)) {
                final byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, read);
                }
            }
            return digest.digest();
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required", exception);
        }
    }

    private static List<Path> files(final Path root) throws IOException {
        final List<Path> files = new ArrayList<>();
        try (var paths = Files.walk(root)) {
            final var iterator = paths.iterator();
            while (iterator.hasNext()) {
                final Path path = iterator.next();
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("Symbolic links are not allowed in a relayed shard tree: " + path);
                }
                if (Files.isRegularFile(path) && !FILE_NAME.equals(path.getFileName().toString())) {
                    files.add(path);
                } else if (Files.exists(path) && !Files.isDirectory(path) && !Files.isRegularFile(path)) {
                    throw new IOException("Unsupported entry in relayed shard tree: " + path);
                }
            }
        }
        files.sort(Comparator.comparing(path -> relative(root, path)));
        return files;
    }

    private static Path requireRoot(final Path treeRoot) throws IOException {
        final Path root = treeRoot.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IOException("Shard transfer tree does not exist: " + root);
        }
        return root;
    }

    private static String relative(final Path root, final Path file) {
        return root.relativize(file).toString().replace('\\', '/');
    }

    private static boolean safeRelative(final String value) {
        if (value == null || value.isBlank() || value.startsWith("/") || value.contains("\\")
            || value.contains("\r") || value.contains("\n")) {
            return false;
        }
        for (final String segment : value.split("/")) {
            if (segment.isBlank() || ".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        return true;
    }

    public record Summary(int files, long bytes) {
    }

    private record Entry(long size, byte[] sha256) {
        private Entry {
            sha256 = sha256.clone();
        }

        @Override
        public byte[] sha256() {
            return this.sha256.clone();
        }
    }
}
