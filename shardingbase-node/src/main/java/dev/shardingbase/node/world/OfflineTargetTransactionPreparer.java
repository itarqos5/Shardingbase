package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/** Creates the target's rollback point before it may accept relayed world data. */
public final class OfflineTargetTransactionPreparer {
    private OfflineTargetTransactionPreparer() {
    }

    public static PreparedTarget prepare(
        final Plan plan,
        final UUID proxyAuthorization,
        final UUID backendAuthorization,
        final boolean backendStoppedCleanly
    ) throws IOException {
        final Path transactionRoot = plan.transactionRoot().toAbsolutePath().normalize()
            .resolve(plan.transactionId().toString());
        final Path journalPath = transactionRoot.resolve("journal.properties");
        final WorldTransactionJournal journal = WorldTransactionJournal.create(journalPath, plan.transactionId());
        try {
            TransactionAuthorization.requireMatching(plan.transactionId(), proxyAuthorization, backendAuthorization);
            journal.advance(TransactionPhase.AUTHORIZED);
            if (!backendStoppedCleanly) {
                throw new IOException("Backend did not confirm a clean stop; target preparation aborted");
            }
            journal.advance(TransactionPhase.BACKEND_STOPPED);
            final boolean absent = Files.notExists(plan.worldRoot());
            final WorldBackupEngine.BackupResult backup = absent
                ? absentBackup(plan.backupRoot(), plan.transactionId())
                : WorldBackupEngine.backup(plan.worldRoot(), plan.backupRoot(), plan.transactionId());
            journal.advance(TransactionPhase.BACKUP_COMPLETE);
            return new PreparedTarget(journalPath, backup, absent);
        } catch (final IOException | RuntimeException exception) {
            try {
                if (journal.phase() != TransactionPhase.FAILED) {
                    journal.advance(TransactionPhase.FAILED);
                }
            } catch (final IOException persistenceFailure) {
                exception.addSuppressed(persistenceFailure);
            }
            throw exception;
        }
    }

    private static WorldBackupEngine.BackupResult absentBackup(
        final Path backupRoot,
        final UUID transactionId
    ) throws IOException {
        final Path root = backupRoot.toAbsolutePath().normalize();
        Files.createDirectories(root);
        final Path destination = root.resolve(transactionId.toString());
        final Path staging = root.resolve("." + transactionId + ".staging");
        if (!destination.startsWith(root) || !staging.startsWith(root)
            || Files.exists(destination) || Files.exists(staging)) {
            throw new IOException("Target absence backup already exists or escapes its configured root");
        }
        Files.createDirectory(staging);
        try {
            Files.writeString(staging.resolve("world.absent"), "The target world did not exist before this transaction.\n");
            try {
                Files.move(staging, destination, StandardCopyOption.ATOMIC_MOVE);
            } catch (final AtomicMoveNotSupportedException exception) {
                throw new IOException("Atomic target absence backup publication is not supported", exception);
            }
            return new WorldBackupEngine.BackupResult(destination, 0L, 1L);
        } finally {
            Files.deleteIfExists(staging.resolve("world.absent"));
            Files.deleteIfExists(staging);
        }
    }

    public record Plan(UUID transactionId, Path worldRoot, Path backupRoot, Path transactionRoot) {
        public Plan {
            if (transactionId == null || worldRoot == null || backupRoot == null || transactionRoot == null) {
                throw new IllegalArgumentException("Target transaction plan fields are required");
            }
            final Path world = worldRoot.toAbsolutePath().normalize();
            final Path backup = backupRoot.toAbsolutePath().normalize();
            final Path transactions = transactionRoot.toAbsolutePath().normalize();
            if (world.equals(backup) || world.equals(transactions) || backup.equals(transactions)
                || world.startsWith(backup) || backup.startsWith(world)
                || world.startsWith(transactions) || transactions.startsWith(world)
                || backup.startsWith(transactions) || transactions.startsWith(backup)) {
                throw new IllegalArgumentException("World, backup, and transaction roots must be non-nested");
            }
        }
    }

    public record PreparedTarget(
        Path journal,
        WorldBackupEngine.BackupResult backup,
        boolean worldInitiallyAbsent
    ) {
        public PreparedTarget {
            if (!Files.isRegularFile(journal) || !Files.isDirectory(backup.path())) {
                throw new IllegalArgumentException("Prepared target is missing its journal or rollback point");
            }
        }
    }
}
