package dev.shardingbase.node;

import dev.shardingbase.protocol.FileTransferCodec;
import dev.shardingbase.protocol.FileTransferCodec.Ack;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Executes routed file I/O away from the persistent session reader. */
final class NodeFileTransferHandler implements AutoCloseable {
    static final String STAGING_ROOT_ENVIRONMENT = "SHARDINGBASE_STAGING_ROOT";

    private final ProxyValidationClient proxy;
    private final ResumableFileReceiver receiver;
    private final ThreadPoolExecutor ioExecutor;

    NodeFileTransferHandler(final ProxyValidationClient proxy, final Path stagingRoot) throws IOException {
        this.proxy = proxy;
        this.receiver = new ResumableFileReceiver(stagingRoot);
        this.ioExecutor = new ThreadPoolExecutor(
            1,
            2,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32),
            task -> Thread.ofPlatform().daemon(true).name("Shardingbase File Transfer").unstarted(task),
            new ThreadPoolExecutor.AbortPolicy()
        );
        proxy.pushHandler(this::submit);
    }

    private void submit(final ProtocolFrame frame) {
        if (frame.channel() != ProtocolChannel.FILE_TRANSFER) {
            this.error(frame, "unsupported routed node channel");
            return;
        }
        try {
            this.ioExecutor.execute(() -> this.handle(frame));
        } catch (RuntimeException _) {
            this.error(frame, "file transfer I/O queue is full");
        }
    }

    private void handle(final ProtocolFrame frame) {
        try {
            final Ack acknowledgement = switch (frame.messageType()) {
                case FILE_BEGIN -> this.receiver.begin(FileTransferCodec.decodeBegin(frame.payload()));
                case FILE_CHUNK -> this.receiver.chunk(FileTransferCodec.decodeChunk(frame.payload()));
                case FILE_COMPLETE -> this.receiver.complete(FileTransferCodec.decodeTransferId(frame.payload()));
                case FILE_ABORT -> {
                    final UUID transferId = FileTransferCodec.decodeTransferId(frame.payload());
                    this.receiver.abort(transferId);
                    yield new Ack(transferId, 0, false, "aborted");
                }
                default -> throw new IOException("unexpected file transfer message");
            };
            this.proxy.respond(frame, MessageType.FILE_ACK, FileTransferCodec.encodeAck(acknowledgement));
        } catch (final IOException exception) {
            this.error(frame, exception.getMessage());
        }
    }

    private void error(final ProtocolFrame frame, final String detail) {
        try {
            this.proxy.respond(
                frame,
                MessageType.ERROR,
                detail.getBytes(StandardCharsets.UTF_8)
            );
        } catch (IOException _) {
        }
    }

    @Override
    public void close() {
        this.ioExecutor.shutdownNow();
    }
}
