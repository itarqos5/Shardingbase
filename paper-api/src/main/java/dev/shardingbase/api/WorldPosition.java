package dev.shardingbase.api;

/**
 * Detached block position safe to carry across an asynchronous boundary.
 *
 * @param worldKey namespaced world key
 * @param x        block X
 * @param y        block Y
 * @param z        block Z
 */
public record WorldPosition(String worldKey, int x, int y, int z) {
}
