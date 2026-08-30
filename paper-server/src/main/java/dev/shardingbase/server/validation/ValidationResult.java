package dev.shardingbase.server.validation;

/** Result returned by the local node after proxy-backed validation. */
public record ValidationResult(boolean accepted, String detail, String peerId, String peerName) {
}
