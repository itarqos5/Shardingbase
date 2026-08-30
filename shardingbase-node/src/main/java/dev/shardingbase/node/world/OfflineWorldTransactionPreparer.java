package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/** Performs the non-destructive, journaled backup and split preparation phases. */
public final class OfflineWorldTransactionPreparer {
    private OfflineWorldTransactionPreparer() {
    }

    public static PreparedTransaction prepare(
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
                throw new IOException("Backend did not confirm a clean stop; transaction preparation aborted");
            }
            journal.advance(TransactionPhase.BACKEND_STOPPED);
            final WorldBackupEngine.BackupResult backup = WorldBackupEngine.backup(
                plan.worldRoot(),
                plan.backupRoot(),
                plan.transactionId()
            );
            journal.advance(TransactionPhase.BACKUP_COMPLETE);
            final Path negative = transactionRoot.resolve("negative");
            final Path positive = transactionRoot.resolve("positive");
            final WorldSplitEngine.SplitSummary summary = WorldSplitEngine.split(
                plan.worldRoot(),
                negative,
                positive,
                plan.axis(),
                plan.cutChunk()
            );
            journal.advance(TransactionPhase.SPLIT_COMPLETE);
            return new PreparedTransaction(journalPath, backup, negative, positive, summary);
        } catch (final IOException | RuntimeException exception) {
            markFailure(journal);
            throw exception;
        }
    }

    private static void markFailure(final WorldTransactionJournal journal) {
        try {
            if (journal.phase() == TransactionPhase.BACKUP_COMPLETE || journal.phase() == TransactionPhase.SPLIT_COMPLETE) {
                journal.advance(TransactionPhase.ROLLED_BACK);
            } else if (journal.phase() != TransactionPhase.ROLLED_BACK) {
                journal.advance(TransactionPhase.FAILED);
            }
        } catch (final IOException exception) {
            System.err.println("Unable to persist failed world transaction state: " + exception.getMessage());
        }
    }

    public record Plan(
        UUID transactionId,
        Path worldRoot,
        Path backupRoot,
        Path transactionRoot,
        ShardAxis axis,
        int cutChunk
    ) {
        public Plan {
            if (transactionId == null || worldRoot == null || backupRoot == null || transactionRoot == null || axis == null) {
                throw new IllegalArgumentException("World transaction plan fields are required");
            }
            final Path normalizedTransactions = transactionRoot.toAbsolutePath().normalize();
            final Path normalizedWorld = worldRoot.toAbsolutePath().normalize();
            final Path normalizedBackup = backupRoot.toAbsolutePath().normalize();
            if (normalizedTransactions.startsWith(normalizedWorld)
                || normalizedWorld.startsWith(normalizedTransactions)
                || normalizedTransactions.startsWith(normalizedBackup)
                || normalizedBackup.startsWith(normalizedTransactions)) {
                throw new IllegalArgumentException("World, backup, and transaction roots must be non-nested");
            }
        }
    }

    public record PreparedTransaction(
        Path journal,
        WorldBackupEngine.BackupResult backup,
        Path negativeHalf,
        Path positiveHalf,
        WorldSplitEngine.SplitSummary summary
    ) {
        public PreparedTransaction {
            if (!Files.isRegularFile(journal) || !Files.isDirectory(backup.path())) {
                throw new IllegalArgumentException("Prepared transaction is missing its journal or backup");
            }
        }
    }
}
