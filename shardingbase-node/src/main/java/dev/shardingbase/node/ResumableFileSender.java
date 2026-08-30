package dev.shardingbase.node;

import dev.shardingbase.node.world.TransferTreeManifest;
import dev.shardingbase.protocol.FileTransferCodec;
import dev.shardingbase.protocol.FileTransferCodec.Ack;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/** Streams a manifested shard tree through Velocity using resumable one-MiB file frames. */
final class ResumableFileSender {
    private final ProxyValidationClient proxy;

    ResumableFileSender(final ProxyValidationClient proxy) {
        this.proxy = proxy;
    }

    TransferSummary sendTree(
        final UUID transactionId,
        final Path treeRoot,
        final String targetNodeId
    ) throws IOException {
        final TransferTreeManifest.Summary manifest = TransferTreeManifest.write(treeRoot);
        long transferred = 0L;
        int files = 0;
        for (final Path file : TransferTreeManifest.filesForTransfer(treeRoot)) {
            final String relative = treeRoot.toAbsolutePath().normalize().relativize(
                file.toAbsolutePath().normalize()
            ).toString().replace('\\', '/');
            final String targetPath = "transactions/" + transactionId + "/world/" + relative;
            final UUID transferId = UUID.nameUUIDFromBytes(
                (transactionId + ":" + relative).getBytes(StandardCharsets.UTF_8)
            );
            transferred = Math.addExact(transferred, this.sendFile(
                transferId, file, targetPath, targetNodeId
            ));
            files++;
        }
        return new TransferSummary(files, transferred, manifest.files(), manifest.bytes());
    }

    private long sendFile(
        final UUID transferId,
        final Path file,
        final String targetPath,
        final String targetNodeId
    ) throws IOException {
        final long size = java.nio.file.Files.size(file);
        Ack acknowledgement = this.ack(this.proxy.request(
            ProtocolChannel.FILE_TRANSFER,
            MessageType.FILE_BEGIN,
            targetNodeId,
            FileTransferCodec.encodeBegin(new FileTransferCodec.Begin(
                transferId,
                targetPath,
                size,
                TransferTreeManifest.sha256(file)
            ))
        ));
        long offset = acknowledgement.nextOffset();
        if (offset > size) {
            throw new IOException("Target acknowledged an offset beyond the source file");
        }
        int noProgress = 0;
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (offset < size) {
                final int length = Math.toIntExact(Math.min(FileTransferCodec.MAX_CHUNK_BYTES, size - offset));
                final ByteBuffer buffer = ByteBuffer.allocate(length);
                channel.position(offset);
                while (buffer.hasRemaining()) {
                    if (channel.read(buffer) < 0) {
                        throw new IOException("Source file ended before its declared size");
                    }
                }
                acknowledgement = this.ack(this.proxy.request(
                    ProtocolChannel.FILE_TRANSFER,
                    MessageType.FILE_CHUNK,
                    targetNodeId,
                    FileTransferCodec.encodeChunk(new FileTransferCodec.Chunk(
                        transferId,
                        offset,
                        buffer.array()
                    ))
                ));
                final long next = acknowledgement.nextOffset();
                if (next < 0 || next > size) {
                    throw new IOException("Target returned an invalid resumable file offset");
                }
                if (next == offset && ++noProgress >= 3) {
                    throw new IOException("Target made no progress after three file-transfer retries");
                }
                if (next != offset) {
                    noProgress = 0;
                }
                offset = next;
            }
        }
        acknowledgement = this.ack(this.proxy.request(
            ProtocolChannel.FILE_TRANSFER,
            MessageType.FILE_COMPLETE,
            targetNodeId,
            FileTransferCodec.encodeTransferId(transferId)
        ));
        if (!acknowledgement.complete() || acknowledgement.nextOffset() != size) {
            throw new IOException("Target did not verify the completed file: " + acknowledgement.detail());
        }
        return size;
    }

    private Ack ack(final ProtocolFrame response) throws IOException {
        if (response.messageType() != MessageType.FILE_ACK) {
            throw new IOException("Target rejected a world file frame");
        }
        return FileTransferCodec.decodeAck(response.payload());
    }

    record TransferSummary(int transferredFiles, long transferredBytes, int contentFiles, long contentBytes) {
    }
}
