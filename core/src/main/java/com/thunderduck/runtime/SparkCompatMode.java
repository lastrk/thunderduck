package com.thunderduck.runtime;

/**
 * Server-level Spark compatibility mode.
 *
 * <p>Controls whether the DuckDB extension providing exact Spark semantics
 * (e.g., decimal division with ROUND_HALF_UP) is loaded and used.
 *
 * <ul>
 *   <li>{@code AUTO} (default) — try to load the extension; use it if available</li>
 *   <li>{@code STRICT} — extension must load or server fails at startup</li>
 *   <li>{@code RELAXED} — never load the extension, vanilla DuckDB only</li>
 * </ul>
 *
 * <p>Set once at startup via {@link #configure(Mode)}, read everywhere via
 * {@link #isStrictMode()} and {@link #isExtensionLoaded()}.
 */
public final class SparkCompatMode {

    public enum Mode { STRICT, RELAXED, AUTO }

    private static volatile Mode configuredMode = Mode.AUTO;
    private static volatile boolean extensionLoaded = false;

    public static void configure(Mode mode) {
        configuredMode = mode;
    }

    public static Mode getConfiguredMode() {
        return configuredMode;
    }

    public static void setExtensionLoaded(boolean loaded) {
        extensionLoaded = loaded;
    }

    public static boolean isExtensionLoaded() {
        return extensionLoaded;
    }

    /** True when the extension is loaded and active (strict-equivalent behavior). */
    public static boolean isStrictMode() {
        return extensionLoaded;
    }

    /** True when running without the extension (vanilla DuckDB types). */
    public static boolean isRelaxedMode() {
        return !extensionLoaded;
    }

    /**
     * Parse a mode string (case-insensitive).
     *
     * @param value "strict", "relaxed", or "auto"
     * @return the parsed Mode
     * @throws IllegalArgumentException if value is not recognized
     */
    public static Mode parse(String value) {
        if (value == null) {
            return Mode.AUTO;
        }
        return switch (value.trim().toLowerCase()) {
            case "strict"  -> Mode.STRICT;
            case "relaxed" -> Mode.RELAXED;
            case "auto"    -> Mode.AUTO;
            default -> throw new IllegalArgumentException(
                "Unknown Spark compatibility mode: '%s'. Valid values: strict, relaxed, auto".formatted(value));
        };
    }

    /**
     * Reset to defaults. Intended for tests only.
     */
    static void reset() {
        configuredMode = Mode.AUTO;
        extensionLoaded = false;
    }

    private SparkCompatMode() {}
}
