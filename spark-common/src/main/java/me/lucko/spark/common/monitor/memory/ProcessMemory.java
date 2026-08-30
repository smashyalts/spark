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

package me.lucko.spark.common.monitor.memory;

import me.lucko.spark.common.monitor.LinuxProc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * fork - the kernel's view of THIS process's memory, as opposed to the JVM's view of itself.
 *
 * <p>{@link MemoryInfo} answers "how much memory does this machine have"; this answers "how much
 * of it is actually charged to us, and can the JVM account for it". That gap is the whole
 * question behind a native memory leak, and it cannot be closed from inside the JVM's own MX
 * beans - they can only report the regions the JVM manages.</p>
 *
 * <p>Everything here reads files the process can open itself, with no shell, no attach API and
 * no container exec. That constraint is deliberate: on a managed host the operator frequently
 * has a plugin and a web panel and nothing else, and diagnostics that require SSH are
 * diagnostics they cannot run.</p>
 *
 * <p>Linux only. Every method degrades to an empty result elsewhere rather than throwing.</p>
 */
public enum ProcessMemory {
    ;

    /**
     * Resident set size of this process, in bytes, or -1 if unavailable.
     *
     * <p>This is the number to trust over a hosting panel's memory gauge. Panels typically
     * display the cgroup's {@code memory.current}, which includes reclaimable kernel page cache
     * and therefore reads close to the limit on any server that does sustained file IO - which
     * is every Minecraft server with a large world. RSS excludes page cache.</p>
     */
    public static long getResidentSetSize() {
        return statusFieldBytes("VmRSS");
    }

    /** Swap used by this process, in bytes, or -1 if unavailable. */
    public static long getSwapUsage() {
        return statusFieldBytes("VmSwap");
    }

    private static long statusFieldBytes(String field) {
        String prefix = field + ":";
        for (String line : LinuxProc.SELF_STATUS.read()) {
            if (line.startsWith(prefix)) {
                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    try {
                        return Long.parseLong(parts[1]) * 1024L;
                    } catch (NumberFormatException e) {
                        return -1;
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Reads /proc/self/smaps_rollup - the kernel's own aggregation of every mapping. Much
     * cheaper than parsing smaps, and it gives the anonymous/file-backed split directly.
     */
    public static Map<String, Long> getSmapsRollup() {
        Map<String, Long> out = new LinkedHashMap<>();
        for (String line : LinuxProc.SELF_SMAPS_ROLLUP.read()) {
            int colon = line.indexOf(':');
            if (colon <= 0 || !line.endsWith(" kB")) {
                continue;
            }
            try {
                out.put(line.substring(0, colon), Long.parseLong(line.substring(colon + 1, line.length() - 3).trim()) * 1024L);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return out;
    }

    /** A single mapping from /proc/self/smaps. */
    public static final class Mapping {
        private final long start;
        private final long end;
        private final String permissions;
        private final String path;
        private long rss;

        Mapping(long start, long end, String permissions, String path) {
            this.start = start;
            this.end = end;
            this.permissions = permissions;
            this.path = path;
        }

        public long start() {
            return this.start;
        }

        public long end() {
            return this.end;
        }

        public long size() {
            return this.end - this.start;
        }

        public long rss() {
            return this.rss;
        }

        public String permissions() {
            return this.permissions;
        }

        public String path() {
            return this.path;
        }

        public boolean anonymous() {
            return this.path.isEmpty();
        }
    }

    /**
     * Parses /proc/self/smaps - the in-process equivalent of {@code pmap -x}.
     *
     * <p>The shape of the result identifies the cause of a large RSS-versus-heap gap faster than
     * any per-allocation profile can, because the three common causes look completely different
     * here: a handful of enormous anonymous regions is direct-buffer or JNI retention, thousands
     * of ~1 MiB regions is a thread leak, and many 64 MiB regions each split into a small
     * readable part and a large {@code ---p} guard is glibc arena retention - which is not a
     * leak at all and cannot be fixed by patching a plugin.</p>
     */
    public static List<Mapping> getMappings() {
        List<Mapping> out = new ArrayList<>();

        // Streamed rather than read into a list first. smaps is roughly 25 lines per mapping, so
        // a large server produces tens of thousands of lines; materialising those as Strings
        // would allocate tens of megabytes on a JVM that, by the time anyone runs this command,
        // is usually already short of memory. Streaming keeps the footprint to the mappings we
        // actually keep.
        try (java.util.stream.Stream<String> lines = LinuxProc.SELF_SMAPS.lines()) {
            parseMappings(lines.iterator(), out);
        } catch (IOException | java.io.UncheckedIOException e) {
            // Files.lines throws UncheckedIOException DURING iteration, not at open, so catching
            // only IOException would let a read failure part-way through smaps escape as an
            // unchecked exception. Returning what was parsed so far is strictly better than
            // failing the whole command over a mapping that vanished mid-read - which is normal,
            // since the process keeps mapping and unmapping while we walk it.
            return out;
        }
        return out;
    }

    private static void parseMappings(java.util.Iterator<String> lines, List<Mapping> out) {
        Mapping current = null;

        while (lines.hasNext()) {
            String line = lines.next();
            if (isHeader(line)) {
                String[] parts = line.trim().split("\\s+", 6);
                String[] range = parts[0].split("-");
                if (range.length != 2) {
                    current = null;
                    continue;
                }
                try {
                    current = new Mapping(
                            Long.parseUnsignedLong(range[0], 16),
                            Long.parseUnsignedLong(range[1], 16),
                            parts.length > 1 ? parts[1] : "",
                            parts.length > 5 ? parts[5].trim() : ""
                    );
                    out.add(current);
                } catch (NumberFormatException e) {
                    current = null;
                }
            } else if (current != null && line.startsWith("Rss:")) {
                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    try {
                        current.rss = Long.parseLong(parts[1]) * 1024L;
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }
        }
    }

    private static boolean isHeader(String line) {
        int dash = line.indexOf('-');
        if (dash <= 0 || dash > 20 || line.isEmpty()) {
            return false;
        }
        char c = line.charAt(0);
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    }

    /** Counts mappings matching the shape of glibc's 64 MiB per-thread-arena reservation. */
    public static int countArenaLikeRegions(List<Mapping> mappings) {
        int count = 0;
        for (Mapping m : mappings) {
            // Match on SIZE alone. An earlier version also required PROT_NONE, on the assumption
            // that an arena heap always keeps a guard portion - but glibc mprotects that portion
            // to READ|WRITE as the heap fills, so a fully committed 64 MiB subheap is entirely
            // rw-p. The permission check therefore excluded precisely the subheaps worth
            // counting, and reported zero on a process holding dozens of them.
            if (m.anonymous() && m.size() >= 60L * 1024 * 1024 && m.size() <= 68L * 1024 * 1024) {
                count++;
            }
        }
        return count;
    }

    /**
     * Resident size of the main arena - the {@code [heap]} mapping grown via brk.
     *
     * <p>Worth reporting separately because MALLOC_ARENA_MAX does not cap it. That tunable
     * limits how many SECONDARY arenas glibc creates; the main arena is always present and grows
     * independently. Growth here is therefore immune to the usual arena fix, and attributing it
     * to "arena fragmentation" would send the reader after a setting that cannot help.</p>
     */
    public static long getMainArenaResident(List<Mapping> mappings) {
        for (Mapping m : mappings) {
            if ("[heap]".equals(m.path())) {
                return m.rss();
            }
        }
        return -1;
    }

    /**
     * Number of open file descriptors, or -1 if unavailable.
     *
     * <p>A descriptor leak is a leak this tool would otherwise miss entirely: each leaked fd costs
     * only a little kernel memory, so RSS barely moves, but the process dies at the ulimit with an
     * error that names nothing useful. Plugins that open files or sockets without closing them in
     * a finally block are the usual cause, and the count climbing steadily is unambiguous.</p>
     */
    public static long getOpenFileDescriptors() {
        try {
            java.io.File dir = new java.io.File("/proc/self/fd");
            String[] entries = dir.list();
            return entries == null ? -1 : entries.length;
        } catch (Exception e) {
            return -1;
        }
    }

    /** Soft limit on open descriptors from /proc/self/limits, or -1. */
    public static long getFileDescriptorLimit() {
        for (String line : LinuxProc.SELF_LIMITS.read()) {
            if (line.startsWith("Max open files")) {
                String[] parts = line.split("\\s+");
                for (String part : parts) {
                    if (part.matches("\\d+")) {
                        return Long.parseLong(part);
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Detects a preloaded allocator replacing the system malloc, or null for plain glibc.
     *
     * <p>This matters because almost every piece of advice about native memory assumes glibc.
     * MALLOC_ARENA_MAX is a glibc tunable that mimalloc and jemalloc simply ignore;
     * malloc_trim - which is what System.trim_native_heap calls - is a glibc entry point that a
     * replacement allocator may not implement at all, so a "nothing was returned" result means
     * nothing. Detecting the allocator is what stops this command confidently prescribing a fix
     * that cannot possibly apply.</p>
     */
    public static String getPreloadedAllocator() {
        for (String line : LinuxProc.SELF_MAPS.read()) {
            if (line.contains("libmimalloc")) {
                return "mimalloc";
            }
            if (line.contains("libjemalloc")) {
                return "jemalloc";
            }
            if (line.contains("libtcmalloc")) {
                return "tcmalloc";
            }
        }
        return null;
    }

    /**
     * Container memory accounting, cgroup v2 first then v1.
     *
     * <p>Inside a container with a private cgroup namespace - the Docker default on a cgroup v2
     * host - the container's own cgroup is mounted at the root, so no container id is needed.</p>
     *
     * <p>Note that {@code memory.current} includes page cache. The useful figure is
     * {@code memory.current - inactive_file}; comparing the two is what settles whether a panel
     * gauge showing a nearly-full container reflects real usage or reclaimable cache.</p>
     */
    public static Map<String, Long> getCgroupMemory() {
        Map<String, Long> out = new LinkedHashMap<>();

        Long current = readSingleValue(LinuxProc.CGROUP_V2_MEMORY_CURRENT);
        if (current != null) {
            out.put("memory.current", current);
            Long max = readSingleValue(LinuxProc.CGROUP_V2_MEMORY_MAX);
            if (max != null) {
                out.put("memory.max", max);
            }
            readStatKeys(LinuxProc.CGROUP_V2_MEMORY_STAT, out,
                    "anon", "file", "inactive_file", "active_file", "slab", "sock", "kernel_stack");
            return out;
        }

        Long usage = readSingleValue(LinuxProc.CGROUP_V1_MEMORY_USAGE);
        if (usage != null) {
            out.put("memory.usage_in_bytes", usage);
            Long limit = readSingleValue(LinuxProc.CGROUP_V1_MEMORY_LIMIT);
            if (limit != null && limit < Long.MAX_VALUE / 2) {
                out.put("memory.limit_in_bytes", limit);
            }
            readStatKeys(LinuxProc.CGROUP_V1_MEMORY_STAT, out,
                    "total_rss", "total_cache", "total_inactive_file");
        }
        return out;
    }

    private static Long readSingleValue(LinuxProc proc) {
        List<String> lines = proc.read();
        if (lines.isEmpty()) {
            return null;
        }
        String value = lines.get(0).trim();
        if (value.equals("max")) {
            return Long.MAX_VALUE;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static void readStatKeys(LinuxProc proc, Map<String, Long> out, String... keys) {
        List<String> lines = proc.read();
        if (lines.isEmpty()) {
            return;
        }
        for (String key : keys) {
            String prefix = key + " ";
            for (String line : lines) {
                if (line.startsWith(prefix)) {
                    try {
                        out.put(key, Long.parseLong(line.substring(prefix.length()).trim()));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                    break;
                }
            }
        }
    }
}
