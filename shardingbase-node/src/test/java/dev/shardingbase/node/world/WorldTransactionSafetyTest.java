package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorldTransactionSafetyTest {
    @Test
    void completedJournalIsTerminal(@TempDir final Path directory) throws Exception {
        final UUID transactionId = UUID.randomUUID();
        final Path path = directory.resolve("journal.properties");
        final WorldTransactionJournal journal = WorldTransactionJournal.create(path, transactionId);
        journal.advance(TransactionPhase.AUTHORIZED);
        journal.advance(TransactionPhase.BACKEND_STOPPED);
        journal.advance(TransactionPhase.BACKUP_COMPLETE);
        journal.advance(TransactionPhase.TARGET_PREPARED);
        journal.advance(TransactionPhase.SOURCE_COMMITTED);
        journal.advance(TransactionPhase.STARTING_TARGET);
        journal.advance(TransactionPhase.STARTING_SOURCE);
        journal.advance(TransactionPhase.COMPLETE);

        assertThrows(IOException.class, () -> journal.advance(TransactionPhase.ROLLED_BACK));
        assertEquals(TransactionPhase.COMPLETE, WorldTransactionJournal.load(path).phase());
    }

    @Test
    void refusesMismatchedDualAuthorization() {
        final UUID transactionId = UUID.randomUUID();
        assertThrows(IOException.class, () -> TransactionAuthorization.requireMatching(
            transactionId,
            transactionId,
            UUID.randomUUID()
        ));
    }

    @Test
    void journalsOnlyLegalDurableTransitions(@TempDir final Path directory) throws Exception {
        final Path path = directory.resolve("transactions/transaction.properties");
        final UUID transactionId = UUID.randomUUID();
        final WorldTransactionJournal journal = WorldTransactionJournal.create(path, transactionId);
        journal.advance(TransactionPhase.AUTHORIZED);
        journal.advance(TransactionPhase.BACKEND_STOPPED);

        final WorldTransactionJournal reloaded = WorldTransactionJournal.load(path);
        assertEquals(transactionId, reloaded.transactionId());
        assertEquals(TransactionPhase.BACKEND_STOPPED, reloaded.phase());
        assertThrows(IOException.class, () -> reloaded.advance(TransactionPhase.SOURCE_COMMITTED));
        assertEquals(TransactionPhase.BACKEND_STOPPED, reloaded.phase());
    }

    @Test
    void createsCompleteHashManifestedBackupBeforePublication(@TempDir final Path directory) throws Exception {
        final Path world = directory.resolve("world");
        final Path backupRoot = directory.resolve("backups");
        Files.createDirectories(world.resolve("region"));
        Files.writeString(world.resolve("level.dat"), "metadata", StandardCharsets.UTF_8);
        Files.write(world.resolve("region/r.0.0.mca"), new byte[] {1, 2, 3});

        final UUID transactionId = UUID.randomUUID();
        final WorldBackupEngine.BackupResult result = WorldBackupEngine.backup(world, backupRoot, transactionId);

        assertEquals(11, result.bytes());
        assertEquals(2, result.files());
        assertEquals("metadata", Files.readString(result.path().resolve("level.dat"), StandardCharsets.UTF_8));
        final String manifest = Files.readString(result.path().resolve("manifest.sha256"), StandardCharsets.UTF_8);
        assertTrue(manifest.contains("level.dat"));
        assertTrue(manifest.contains("region/r.0.0.mca"));
        assertFalse(Files.exists(backupRoot.resolve("." + transactionId + ".staging")));
    }

    @Test
    void preparesOnlyAfterDualAuthorizationCleanStopAndFullBackup(@TempDir final Path directory) throws Exception {
        final Path world = directory.resolve("world");
        writeRegion(world.resolve("region/r.0.0.mca"));
        Files.writeString(world.resolve("level.dat"), "metadata", StandardCharsets.UTF_8);
        final UUID transactionId = UUID.randomUUID();
        final OfflineWorldTransactionPreparer.Plan plan = new OfflineWorldTransactionPreparer.Plan(
            transactionId,
            world,
            directory.resolve("backups"),
            directory.resolve("transactions"),
            ShardAxis.X,
            1
        );

        final OfflineWorldTransactionPreparer.PreparedTransaction prepared = OfflineWorldTransactionPreparer.prepare(
            plan,
            transactionId,
            transactionId,
            true
        );

        assertEquals(TransactionPhase.SPLIT_COMPLETE, WorldTransactionJournal.load(prepared.journal()).phase());
        assertTrue(Files.isRegularFile(prepared.backup().path().resolve("level.dat")));
        assertEquals(1, prepared.summary().negativeChunkEntries());
        assertEquals(1, prepared.summary().positiveChunkEntries());
        assertTrue(Files.isRegularFile(prepared.negativeHalf().resolve("region/r.0.0.mca")));
        assertTrue(Files.isRegularFile(prepared.positiveHalf().resolve("region/r.0.0.mca")));
        assertEquals("metadata", Files.readString(prepared.negativeHalf().resolve("level.dat")));
        assertEquals("metadata", Files.readString(prepared.positiveHalf().resolve("level.dat")));
    }

    @Test
    void journalsAnAbsentTargetRollbackPointBeforeRelay(@TempDir final Path directory) throws Exception {
        final UUID transactionId = UUID.randomUUID();
        final OfflineTargetTransactionPreparer.PreparedTarget prepared =
            OfflineTargetTransactionPreparer.prepare(
                new OfflineTargetTransactionPreparer.Plan(
                    transactionId,
                    directory.resolve("missing-world"),
                    directory.resolve("backups"),
                    directory.resolve("transactions")
                ),
                transactionId,
                transactionId,
                true
            );

        assertTrue(prepared.worldInitiallyAbsent());
        assertTrue(Files.isRegularFile(prepared.backup().path().resolve("world.absent")));
        assertEquals(TransactionPhase.BACKUP_COMPLETE, WorldTransactionJournal.load(prepared.journal()).phase());
    }

    @Test
    void writesBackendOwnershipManifestAtomically(@TempDir final Path directory) throws Exception {
        final Path world = directory.resolve("world");
        Files.createDirectories(world);
        final UUID transactionId = UUID.randomUUID();
        final Path path = ShardManifestWriter.write(world, new ShardManifestWriter.Manifest(
            "minecraft:overworld",
            UUID.randomUUID(),
            transactionId,
            ShardAxis.Z,
            -4,
            ShardSide.POSITIVE,
            "peer-a"
        ));

        final Properties properties = new Properties();
        try (var input = Files.newInputStream(path)) {
            properties.load(input);
        }
        assertEquals("1", properties.getProperty("format-version"));
        assertEquals(transactionId.toString(), properties.getProperty("transaction-id"));
        assertTrue(properties.containsKey("world-id"));
        assertEquals("POSITIVE", properties.getProperty("owned-side"));
    }

    @Test
    void atomicallyInstallsAndRollsBackAVerifiedShardTree(@TempDir final Path directory) throws Exception {
        final Path world = directory.resolve("world");
        final Path prepared = directory.resolve("prepared");
        final Path transaction = directory.resolve("transaction");
        Files.createDirectories(world);
        Files.createDirectories(prepared);
        Files.createDirectories(transaction);
        Files.writeString(world.resolve("level.dat"), "original");
        Files.writeString(prepared.resolve("level.dat"), "sharded");
        TransferTreeManifest.write(prepared);
        final UUID transactionId = UUID.randomUUID();

        final WorldInstallationEngine.InstalledWorld installed = WorldInstallationEngine.install(
            world,
            prepared,
            transaction,
            "target",
            new ShardManifestWriter.Manifest(
                "minecraft:overworld",
                UUID.randomUUID(),
                transactionId,
                ShardAxis.X,
                0,
                ShardSide.POSITIVE,
                "peer"
            )
        );

        assertEquals("sharded", Files.readString(world.resolve("level.dat")));
        assertTrue(Files.isRegularFile(world.resolve(ShardManifestWriter.FILE_NAME)));
        assertEquals("original", Files.readString(installed.retiredOriginal().resolve("level.dat")));

        WorldInstallationEngine.rollback(
            world,
            installed.retiredOriginal(),
            false,
            transaction.resolve("target-failed")
        );
        assertEquals("original", Files.readString(world.resolve("level.dat")));
        assertEquals("sharded", Files.readString(transaction.resolve("target-failed/level.dat")));
    }

    private static void writeRegion(final Path path) throws IOException {
        Files.createDirectories(path.getParent());
        final ByteBuffer file = ByteBuffer.allocate(
            RegionFileSplitter.HEADER_BYTES + 2 * RegionFileSplitter.SECTOR_BYTES
        ).order(ByteOrder.BIG_ENDIAN);
        for (int entry = 0; entry < 2; entry++) {
            file.putInt(entry * Integer.BYTES, (2 + entry) << 8 | 1);
            file.putInt(RegionFileSplitter.SECTOR_BYTES + entry * Integer.BYTES, 100 + entry);
            file.putInt(RegionFileSplitter.HEADER_BYTES + entry * RegionFileSplitter.SECTOR_BYTES, 2);
            file.put(RegionFileSplitter.HEADER_BYTES + entry * RegionFileSplitter.SECTOR_BYTES + 4, (byte) 3);
        }
        Files.write(path, file.array());
    }
}
