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

import me.lucko.spark.common.monitor.DiagnosticCommand;
import me.lucko.spark.proto.SparkSamplerProtos;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * fork - one reading of every memory figure obtainable from inside the process.
 *
 * <p>Exists as its own class rather than living in the command that prints it because the same
 * reading is attached to every exported profile. A leak tree is not interpretable without it:
 * "600 KB of unfreed native allocations" describes a healthy server and a catastrophically
 * broken one equally well, and only the gap between resident size and what the JVM can account
 * for tells you which you are looking at.</p>
 *
 * <p>The default capture is cheap - {@code /proc/self/status}, {@code smaps_rollup}, the cgroup
 * files and some MX bean calls, single-digit milliseconds. The expensive part, parsing
 * {@code /proc/self/smaps} for individual mappings, is opt-in via
 * {@link #capture(boolean, boolean)} because its cost scales with the process's resident page
 * count and can reach hundreds of milliseconds on a large heap.</p>
 */
public final class ProcessMemorySnapshot {

    /** Mappings included in an export. Enough to identify a pattern, small enough to upload. */
    private static final int MAX_EXPORTED_MAPPINGS = 64;

    private long timestamp;
    private long rss = -1;
    private long swap = -1;
    private Map<String, Long> smapsRollup = new LinkedHashMap<>();
    private Map<String, Long> cgroup = new LinkedHashMap<>();

    private long heapUsed;
    private long heapCommitted;
    private long heapMax;
    private long nonHeapCommitted;
    private final Map<String, Long> nonHeapPools = new LinkedHashMap<>();
    private long directUsed;
    private long directCount;
    private long mappedUsed;
    private long mappedCount;
    private long nettyDirect = -1;
    private long nettyMaxDirect = -1;
    private long nettyArenas = -1;
    private int threads;
    private int peakThreads;
    private long threadStackSize = -1;
    private long loadedClasses;

    private boolean alwaysPreTouch;
    private String maxDirectMemorySize;
    private String mallocArenaMax;
    private int availableProcessors;

    private int arenaLikeRegions = -1;
    private int totalMappings = -1;
    private List<ProcessMemory.Mapping> largestMappings;

    private boolean nmtEnabled;
    private String nmtSummary;

    /**
     * Cheap capture: no smaps parsing, and no NMT summary is retained.
     *
     * <p>It does still ask whether NMT is enabled, which internally costs one summary walk the
     * first time and nothing afterwards, since the answer is fixed at JVM startup and cached.</p>
     */
    public static ProcessMemorySnapshot capture() {
        return capture(false, false);
    }

    public static ProcessMemorySnapshot capture(boolean includeMappings, boolean includeNmt) {
        ProcessMemorySnapshot s = new ProcessMemorySnapshot();
        s.timestamp = System.currentTimeMillis();

        s.rss = ProcessMemory.getResidentSetSize();
        s.swap = ProcessMemory.getSwapUsage();
        s.smapsRollup = ProcessMemory.getSmapsRollup();
        s.cgroup = ProcessMemory.getCgroupMemory();

        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        // One MemoryUsage, three fields. Each getHeapMemoryUsage() call queries the VM afresh, so
        // reading it three times can produce a triple that never coexisted - used above committed
        // after a GC, or committed above max after an expansion. unaccounted() subtracts
        // heapCommitted from RSS and every verdict in OffHeapInvestigation turns on that figure.
        java.lang.management.MemoryUsage heap = memory.getHeapMemoryUsage();
        s.heapUsed = heap.getUsed();
        s.heapCommitted = heap.getCommitted();
        s.heapMax = heap.getMax();
        s.nonHeapCommitted = memory.getNonHeapMemoryUsage().getCommitted();

        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.NON_HEAP && pool.getUsage() != null) {
                s.nonHeapPools.put(pool.getName(), pool.getUsage().getCommitted());
            }
        }

        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if (pool.getName().equals("direct")) {
                s.directUsed = pool.getMemoryUsed();
                s.directCount = pool.getCount();
            } else if (pool.getName().equals("mapped")) {
                s.mappedUsed = pool.getMemoryUsed();
                s.mappedCount = pool.getCount();
            }
        }

        s.nettyDirect = nettyCounter("usedDirectMemory");
        if (s.nettyDirect < 0) {
            s.nettyDirect = nettyPooledDirect(); // counter often unavailable; the pool is not
        }
        s.nettyArenas = nettyDirectArenaCount();
        s.nettyMaxDirect = nettyCounter("maxDirectMemory");

        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        s.threads = threadBean.getThreadCount();
        s.peakThreads = threadBean.getPeakThreadCount();

        String stackSize = DiagnosticCommand.getVmOption("ThreadStackSize");
        if (stackSize != null) {
            try {
                s.threadStackSize = Long.parseLong(stackSize.trim()) * 1024L;
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        s.loadedClasses = ManagementFactory.getClassLoadingMXBean().getLoadedClassCount();
        s.alwaysPreTouch = "true".equals(DiagnosticCommand.getVmOption("AlwaysPreTouch"));
        s.maxDirectMemorySize = DiagnosticCommand.getVmOption("MaxDirectMemorySize");
        s.mallocArenaMax = System.getenv("MALLOC_ARENA_MAX");
        s.availableProcessors = Runtime.getRuntime().availableProcessors();

        if (includeMappings) {
            List<ProcessMemory.Mapping> mappings = ProcessMemory.getMappings();
            s.totalMappings = mappings.size();
            s.arenaLikeRegions = ProcessMemory.countArenaLikeRegions(mappings);
            mappings.sort(Comparator.comparingLong(ProcessMemory.Mapping::rss).reversed());
            // Copy, not subList. A subList is a VIEW over the backing list, so retaining one pins
            // every Mapping parsed from smaps - thousands of objects - for the life of the
            // snapshot, on the profile export path. A memory diagnostic that itself retains
            // memory proportional to the process it is measuring is the wrong shape of tool.
            s.largestMappings = new ArrayList<>(
                    mappings.subList(0, Math.min(MAX_EXPORTED_MAPPINGS, mappings.size())));
        }

        s.nmtEnabled = DiagnosticCommand.isNativeMemoryTrackingEnabled();
        if (includeNmt && s.nmtEnabled) {
            s.nmtSummary = DiagnosticCommand.execute("VM.native_memory", "summary", "scale=MB");
        }

        return s;
    }

    /**
     * Sums the chunks held by Netty's pooled direct arenas.
     *
     * <p>Needed because {@code PlatformDependent.usedDirectMemory()} returns -1 unless the
     * no-Cleaner path is active, and because the JDK's own direct buffer pool cannot see this
     * memory at all: Netty allocates pooled chunks through Unsafe, deliberately bypassing the
     * Cleaner and therefore BufferPoolMXBean. A server can hold gigabytes in Netty arenas while
     * the JVM reports a few megabytes of direct buffers - precisely the reading that sends an
     * investigation in the wrong direction. Chunks are never returned to the OS once an arena
     * takes them, so this is the ceiling the process has reached, not current occupancy.</p>
     */
    private static long nettyPooledDirect() {
        try {
            Class<?> allocator = Class.forName("io.netty.buffer.PooledByteBufAllocator");
            Object def = allocator.getField("DEFAULT").get(null);
            Object metric = allocator.getMethod("metric").invoke(def);

            Object chunkSizeObj = metric.getClass().getMethod("chunkSize").invoke(metric);
            long chunkSize = chunkSizeObj instanceof Number ? ((Number) chunkSizeObj).longValue() : -1;
            if (chunkSize <= 0) {
                return -1;
            }

            Object arenas = metric.getClass().getMethod("directArenas").invoke(metric);
            if (!(arenas instanceof Iterable)) {
                return -1;
            }

            long total = 0;
            for (Object arena : (Iterable<?>) arenas) {
                Object lists = arena.getClass().getMethod("chunkLists").invoke(arena);
                if (!(lists instanceof Iterable)) {
                    continue;
                }
                for (Object list : (Iterable<?>) lists) {
                    if (list instanceof Iterable) {
                        for (Object chunk : (Iterable<?>) list) {
                            if (chunk != null) {
                                total += chunkSize;
                            }
                        }
                    }
                }
            }
            return total;
        } catch (Throwable t) {
            return -1;
        }
    }

    /** Number of pooled direct arenas Netty created - defaults to 2x cores. */
    private static long nettyDirectArenaCount() {
        try {
            Class<?> allocator = Class.forName("io.netty.buffer.PooledByteBufAllocator");
            Object def = allocator.getField("DEFAULT").get(null);
            Object metric = allocator.getMethod("metric").invoke(def);
            Object n = metric.getClass().getMethod("numDirectArenas").invoke(metric);
            return n instanceof Number ? ((Number) n).longValue() : -1;
        } catch (Throwable t) {
            return -1;
        }
    }

    private static long nettyCounter(String method) {
        try {
            Class<?> platformDependent = Class.forName("io.netty.util.internal.PlatformDependent");
            Object value = platformDependent.getMethod(method).invoke(null);
            return value instanceof Number ? ((Number) value).longValue() : -1;
        } catch (Throwable t) {
            return -1;
        }
    }

    /**
     * Resident size minus everything nameable, or {@link Long#MIN_VALUE} if RSS is unavailable.
     *
     * <p>A negative result is legal and common: without {@code AlwaysPreTouch} the heap is
     * committed but not fully resident, so the accounted total can exceed RSS. That is why the
     * unavailable sentinel is {@link Long#MIN_VALUE} rather than -1.</p>
     */
    public long unaccounted() {
        if (this.rss < 0) {
            return Long.MIN_VALUE;
        }
        long stacks = (long) this.threads * (this.threadStackSize > 0 ? this.threadStackSize : 1024L * 1024L);
        return this.rss - (this.heapCommitted + this.nonHeapCommitted + this.directUsed + stacks);
    }

    public SparkSamplerProtos.ProcessMemoryData toProto() {
        long unaccounted = unaccounted();

        SparkSamplerProtos.ProcessMemoryData.Builder proto = SparkSamplerProtos.ProcessMemoryData.newBuilder()
                .setTimestamp(this.timestamp)
                .setRss(Math.max(0, this.rss))
                .setSwap(Math.max(0, this.swap))
                .setHeapUsed(this.heapUsed)
                .setHeapCommitted(this.heapCommitted)
                .setHeapMax(this.heapMax)
                .setNonHeapCommitted(this.nonHeapCommitted)
                .setNioDirectUsed(this.directUsed)
                .setNioDirectCount(this.directCount)
                .setNioMappedUsed(this.mappedUsed)
                // -1 means "could not measure", not "zero bytes". Clamping keeps a consumer from
                // charting minus one byte of Netty memory; has_netty_direct is implied by > 0.
                .setNettyDirect(Math.max(0, this.nettyDirect))
                .setNettyMaxDirect(Math.max(0, this.nettyMaxDirect))
                .setThreads(this.threads)
                .setPeakThreads(this.peakThreads)
                // clamped for the same reason as the fields above: -1 means "the VM option could
                // not be read", and a consumer multiplying threads by a negative stack size gets
                // a negative thread-stack total
                .setThreadStackSize(Math.max(0, this.threadStackSize))
                .setLoadedClasses(this.loadedClasses)
                // 0 rather than the Long.MIN_VALUE sentinel: the proto has no way to express
                // "unavailable", and shipping the sentinel means every consumer - the viewer, the
                // analysis scripts - reads it as a real measurement of -9.2 exabytes.
                .setUnaccounted(unaccounted == Long.MIN_VALUE ? 0 : unaccounted)
                .setAlwaysPreTouch(this.alwaysPreTouch)
                .setAvailableProcessors(this.availableProcessors)
                .setNmtEnabled(this.nmtEnabled);

        proto.putAllSmapsRollup(this.smapsRollup);
        proto.putAllCgroup(this.cgroup);
        proto.putAllNonHeapPools(this.nonHeapPools);

        if (this.maxDirectMemorySize != null) {
            proto.setMaxDirectMemorySize(this.maxDirectMemorySize);
        }
        if (this.mallocArenaMax != null) {
            proto.setMallocArenaMax(this.mallocArenaMax);
        }
        if (this.nmtSummary != null) {
            proto.setNmtSummary(this.nmtSummary);
        }
        if (this.totalMappings >= 0) {
            proto.setTotalMappings(this.totalMappings);
            proto.setArenaLikeRegions(this.arenaLikeRegions);
            for (ProcessMemory.Mapping mapping : this.largestMappings) {
                proto.addLargestMappings(SparkSamplerProtos.MemoryMapping.newBuilder()
                        .setSize(mapping.size())
                        .setRss(mapping.rss())
                        .setPermissions(mapping.permissions())
                        .setPath(mapping.path())
                        .build());
            }
        }

        return proto.build();
    }

    public long rss() {
        return this.rss;
    }

    public long swap() {
        return this.swap;
    }

    public long heapUsed() {
        return this.heapUsed;
    }

    public long heapCommitted() {
        return this.heapCommitted;
    }

    public long heapMax() {
        return this.heapMax;
    }

    public long nonHeapCommitted() {
        return this.nonHeapCommitted;
    }

    public Map<String, Long> nonHeapPools() {
        return this.nonHeapPools;
    }

    public long directUsed() {
        return this.directUsed;
    }

    public long directCount() {
        return this.directCount;
    }

    public long mappedUsed() {
        return this.mappedUsed;
    }

    public long mappedCount() {
        return this.mappedCount;
    }

    public long nettyArenas() {
        return this.nettyArenas;
    }

    public long nettyDirect() {
        return this.nettyDirect;
    }

    public long nettyMaxDirect() {
        return this.nettyMaxDirect;
    }

    public int threads() {
        return this.threads;
    }

    public int peakThreads() {
        return this.peakThreads;
    }

    public long threadStackSize() {
        return this.threadStackSize;
    }

    public long loadedClasses() {
        return this.loadedClasses;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public Map<String, Long> cgroup() {
        return this.cgroup;
    }

    public Map<String, Long> smapsRollup() {
        return this.smapsRollup;
    }

    public boolean nmtEnabled() {
        return this.nmtEnabled;
    }

    public String maxDirectMemorySize() {
        return this.maxDirectMemorySize;
    }

    public String mallocArenaMax() {
        return this.mallocArenaMax;
    }

    public int availableProcessors() {
        return this.availableProcessors;
    }
}
