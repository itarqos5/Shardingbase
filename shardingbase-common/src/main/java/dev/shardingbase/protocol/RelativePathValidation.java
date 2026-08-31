package dev.shardingbase.protocol;

/** Shared validation for server-root-relative protocol paths. */
final class RelativePathValidation {
    private RelativePathValidation() {
    }

    static boolean worldDirectory(final String value) {
        if (value == null || value.isBlank() || value.startsWith("/") || value.contains("\\")
            || value.length() > 512 || value.matches("^[A-Za-z]:/.*")
            || value.codePoints().anyMatch(Character::isISOControl)) {
            return false;
        }
        for (final String segment : value.split("/")) {
            if (segment.isBlank() || ".".equals(segment) || "..".equals(segment)
                || segment.endsWith(".") || segment.endsWith(" ")) {
                return false;
            }
        }
        return true;
    }
}
