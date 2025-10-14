package com.catalyst2sql.runtime;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.util.Objects;

/**
 * Detects hardware capabilities for optimization.
 *
 * <p>This class profiles the system hardware to provide recommendations
 * for optimal DuckDB configuration, including thread count and memory limits.
 *
 * <p>Hardware detection includes:
 * <ul>
 *   <li>CPU core count</li>
 *   <li>Total physical memory</li>
 *   <li>CPU architecture (x86-64, ARM)</li>
 *   <li>SIMD instruction support (AVX-512, NEON)</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   HardwareProfile profile = HardwareProfile.detect();
 *   int threads = profile.recommendedThreadCount();
 *   String memLimit = profile.recommendedMemoryLimit();
 * </pre>
 *
 * @see DuckDBConnectionManager
 */
public class HardwareProfile {

    private final int cpuCores;
    private final long totalMemoryBytes;
    private final boolean avx512Supported;
    private final boolean neonSupported;
    private final String architecture;

    /**
     * Creates a hardware profile.
     *
     * @param cpuCores the number of CPU cores
     * @param totalMemory the total physical memory in bytes
     * @param avx512 whether AVX-512 is supported
     * @param neon whether ARM NEON is supported
     * @param arch the CPU architecture string
     */
    private HardwareProfile(int cpuCores, long totalMemory,
                           boolean avx512, boolean neon, String arch) {
        this.cpuCores = cpuCores;
        this.totalMemoryBytes = totalMemory;
        this.avx512Supported = avx512;
        this.neonSupported = neon;
        this.architecture = Objects.requireNonNull(arch, "arch must not be null");
    }

    /**
     * Detects the hardware profile of the current system.
     *
     * <p>This method queries the JVM and operating system to determine
     * hardware capabilities. The detection is performed once and cached.
     *
     * @return the detected hardware profile
     */
    public static HardwareProfile detect() {
        // Detect CPU cores
        int cores = Runtime.getRuntime().availableProcessors();

        // Detect total physical memory
        long memory;
        try {
            OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
                OperatingSystemMXBean.class);
            memory = osBean.getTotalPhysicalMemorySize();
        } catch (Exception e) {
            // Fallback to max heap if physical memory detection fails
            memory = Runtime.getRuntime().maxMemory() * 4; // Estimate
        }

        // Detect architecture
        String arch = System.getProperty("os.arch").toLowerCase();

        // Detect SIMD support based on architecture
        // Note: This is a heuristic - actual SIMD detection requires native code
        boolean avx512 = arch.contains("x86") || arch.contains("amd64");
        boolean neon = arch.contains("aarch64") || arch.contains("arm");

        return new HardwareProfile(cores, memory, avx512, neon, arch);
    }

    /**
     * Returns the recommended thread count for DuckDB.
     *
     * <p>The recommendation uses 75% of available cores, with a minimum
     * of 1 and maximum of 16 to avoid oversubscription.
     *
     * @return the recommended thread count
     */
    public int recommendedThreadCount() {
        // Use 75% of cores, minimum 1, maximum 16
        return Math.max(1, Math.min(16, (cpuCores * 3) / 4));
    }

    /**
     * Returns the recommended memory limit for DuckDB.
     *
     * <p>The recommendation uses 75% of total physical memory, formatted
     * as a string suitable for DuckDB's memory_limit setting.
     *
     * @return the memory limit string (e.g., "8GB", "512MB")
     */
    public String recommendedMemoryLimit() {
        // Use 75% of total memory
        long limitBytes = (totalMemoryBytes * 3) / 4;
        return formatBytes(limitBytes);
    }

    /**
     * Formats a byte count as a human-readable string.
     *
     * @param bytes the number of bytes
     * @return the formatted string (e.g., "8GB", "512MB")
     */
    private String formatBytes(long bytes) {
        if (bytes >= 1024L * 1024 * 1024) {
            return (bytes / (1024L * 1024 * 1024)) + "GB";
        } else if (bytes >= 1024L * 1024) {
            return (bytes / (1024L * 1024)) + "MB";
        } else if (bytes >= 1024) {
            return (bytes / 1024) + "KB";
        } else {
            return bytes + "B";
        }
    }

    /**
     * Returns the number of CPU cores.
     *
     * @return the CPU core count
     */
    public int cpuCores() {
        return cpuCores;
    }

    /**
     * Returns the total physical memory in bytes.
     *
     * @return the total memory
     */
    public long totalMemoryBytes() {
        return totalMemoryBytes;
    }

    /**
     * Returns whether AVX-512 instructions are likely supported.
     *
     * <p>This is a heuristic based on architecture detection.
     * For accurate detection, native code would be required.
     *
     * @return true if AVX-512 is likely supported
     */
    public boolean supportsAVX512() {
        return avx512Supported;
    }

    /**
     * Returns whether ARM NEON instructions are likely supported.
     *
     * <p>This is a heuristic based on architecture detection.
     *
     * @return true if NEON is likely supported
     */
    public boolean supportsNEON() {
        return neonSupported;
    }

    /**
     * Returns the CPU architecture string.
     *
     * @return the architecture (e.g., "amd64", "aarch64")
     */
    public String architecture() {
        return architecture;
    }

    /**
     * Returns a string representation of this hardware profile.
     *
     * @return a human-readable description
     */
    @Override
    public String toString() {
        return String.format(
            "HardwareProfile(cores=%d, memory=%s, arch=%s, avx512=%s, neon=%s)",
            cpuCores,
            formatBytes(totalMemoryBytes),
            architecture,
            avx512Supported,
            neonSupported
        );
    }
}
