/*
 * This file is part of spark.
 *
 *  Copyright (c) lucko (Luck) <luck@lucko.me>
 *  Copyright (c) contributors
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package me.lucko.spark.common.monitor;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream; // fork

/**
 * Utility for reading from /proc/ on Linux systems.
 */
public enum LinuxProc {

    /**
     * Information about the system CPU.
     */
    CPUINFO("/proc/cpuinfo"),

    /**
     * Information about the system memory.
     */
    MEMINFO("/proc/meminfo"),

    /**
     * Information about the system network usage.
     */
    NET_DEV("/proc/net/dev"),

    /**
     * Information about the operating system distro.
     */
    OSINFO("/etc/os-release"),

    // fork - the entries below describe THIS process and its container, rather than the machine.
    // They back the off-heap diagnostics in ProcessMemory: the JVM's own MX beans can only report
    // regions the JVM manages, so the gap between those and the process's real resident size is
    // invisible without reading the kernel's view directly.

    /**
     * Status of the current process, including its resident set size. // fork
     */
    SELF_STATUS("/proc/self/status"),

    /**
     * Kernel-aggregated summary of every memory mapping of the current process. // fork
     */
    SELF_SMAPS_ROLLUP("/proc/self/smaps_rollup"),

    /**
     * Per-mapping memory detail for the current process - the source for a pmap-style view. // fork
     */
    SELF_SMAPS("/proc/self/smaps"),

    /**
     * Mapping list for the current process - cheaper than smaps when only paths are needed. // fork
     */
    SELF_MAPS("/proc/self/maps"),

    /**
     * Resource limits for the current process - used to warn before a descriptor leak hits the
     * ceiling rather than after. // fork
     */
    SELF_LIMITS("/proc/self/limits"),

    /**
     * Current memory usage of the container cgroup, v2 layout. Includes page cache. // fork
     */
    CGROUP_V2_MEMORY_CURRENT("/sys/fs/cgroup/memory.current"),

    /**
     * Memory limit of the container cgroup, v2 layout. // fork
     */
    CGROUP_V2_MEMORY_MAX("/sys/fs/cgroup/memory.max"),

    /**
     * Memory breakdown of the container cgroup, v2 layout. // fork
     */
    CGROUP_V2_MEMORY_STAT("/sys/fs/cgroup/memory.stat"),

    /**
     * Current memory usage of the container cgroup, v1 layout. // fork
     */
    CGROUP_V1_MEMORY_USAGE("/sys/fs/cgroup/memory/memory.usage_in_bytes"),

    /**
     * Memory limit of the container cgroup, v1 layout. // fork
     */
    CGROUP_V1_MEMORY_LIMIT("/sys/fs/cgroup/memory/memory.limit_in_bytes"),

    /**
     * Memory breakdown of the container cgroup, v1 layout. // fork
     */
    CGROUP_V1_MEMORY_STAT("/sys/fs/cgroup/memory/memory.stat");

    private final Path path;

    LinuxProc(String path) {
        this.path = resolvePath(path);
    }

    private static @Nullable Path resolvePath(String path) {
        try {
            Path p = Paths.get(path);
            if (Files.isReadable(p)) {
                return p;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    /**
     * Streams the file line by line. // fork
     *
     * <p>Exists for /proc files that are large enough that materialising them as a list is a
     * problem in itself - smaps on a big JVM is tens of thousands of lines, and this runs on a
     * server that may already be short of memory. The caller must close the stream.</p>
     */
    public Stream<@NonNull String> lines() throws IOException { // fork
        if (this.path == null) {
            return Stream.empty();
        }
        return Files.lines(this.path, StandardCharsets.UTF_8);
    }

    public @NonNull List<String> read() {
        if (this.path != null) {
            try {
                return Files.readAllLines(this.path, StandardCharsets.UTF_8);
            } catch (IOException e) {
                // ignore
            }
        }

        return Collections.emptyList();
    }

}
