package dev.shardingbase.api;

/**
 * Typed result of an asynchronous remote operation.
 *
 * @param <T> result value type
 */
public sealed interface RemoteResult<T>
    permits RemoteResult.Success, RemoteResult.Timeout, RemoteResult.Unavailable,
    RemoteResult.ValidationFailure, RemoteResult.RemoteFailure {
    /**
     * Successful result.
     *
     * @param value returned value
     * @param <T>   value type
     */
    record Success<T>(T value) implements RemoteResult<T> {
    }

    /**
     * Remote peer did not answer before the operation deadline.
     *
     * @param message failure detail
     * @param <T>     expected value type
     */
    record Timeout<T>(String message) implements RemoteResult<T> {
    }

    /**
     * Distributed features or the target peer are unavailable.
     *
     * @param message failure detail
     * @param <T>     expected value type
     */
    record Unavailable<T>(String message) implements RemoteResult<T> {
    }

    /**
     * The request failed local or remote validation.
     *
     * @param message failure detail
     * @param <T>     expected value type
     */
    record ValidationFailure<T>(String message) implements RemoteResult<T> {
    }

    /**
     * The peer accepted the request but failed while executing it.
     *
     * @param message remote failure detail
     * @param <T>     expected value type
     */
    record RemoteFailure<T>(String message) implements RemoteResult<T> {
    }
}
