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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * fork - per-arena statistics from glibc's own allocator, via {@code System.native_heap_info}.
 *
 * <p>This is the measurement that closes the largest blind spot in off-heap diagnosis. When a
 * JVM's resident size vastly exceeds what it can account for, and the leak detector reports
 * almost nothing unfreed, the usual conclusion is "some native library is leaking". A far more
 * common explanation on a many-threaded server is that nothing is leaking at all: glibc gives
 * each thread its own arena, grows each arena with 64 MiB heaps, and never returns them. The
 * allocations inside are freed correctly - which is exactly why a malloc leak profiler sees
 * nothing - but the memory stays charged to the process forever.</p>
 *
 * <p>Telling those apart needs the allocator's own accounting, and glibc exposes it through
 * {@code malloc_info(3)}. Per arena it reports the bytes taken from the OS, the bytes currently
 * free within them, and how many 64 MiB subheaps are chained together. A large total with a high
 * free fraction spread over many arenas is fragmentation, and the fix is a tunable. The same
 * total with a low free fraction is a real leak, and the fix is finding the caller. Without this
 * one call those two cases look identical from every other vantage point available in-process.</p>
 */
public final class GlibcArenaInfo {

    private static final Pattern HEAP = Pattern.compile("<heap nr=\"(\\d+)\">(.*?)</heap>", Pattern.DOTALL);
    private static final Pattern SYSTEM_CURRENT = Pattern.compile("<system type=\"current\" size=\"(\\d+)\"");
    private static final Pattern REST = Pattern.compile("<total type=\"rest\" count=\"\\d+\" size=\"(\\d+)\"");
    private static final Pattern FAST = Pattern.compile("<total type=\"fast\" count=\"\\d+\" size=\"(\\d+)\"");
    private static final Pattern SUBHEAPS = Pattern.compile("<aspace type=\"subheaps\" size=\"(\\d+)\"");
    /** Large allocations served by mmap rather than an arena - invisible in the per-heap blocks. */
    private static final Pattern MMAP_TOTAL = Pattern.compile("<total type=\"mmap\" count=\"(\\d+)\" size=\"(\\d+)\"");
    private static final Pattern HEAP_BLOCK = Pattern.compile("<heap nr=\"\\d+\">.*?</heap>", Pattern.DOTALL);

    private final Map<Integer, long[]> perArena; // arena nr -> {systemBytes, freeBytes, subheaps}
    private final long mmapBytes;
    private final long mmapCount;
    private final int arenas;
    private final long systemBytes;
    private final long freeBytes;
    private final int subheaps;
    private final boolean available;

    private GlibcArenaInfo(int arenas, long systemBytes, long freeBytes, int subheaps, boolean available,
                           Map<Integer, long[]> perArena, long mmapBytes, long mmapCount) {
        this.mmapBytes = mmapBytes;
        this.mmapCount = mmapCount;
        this.perArena = perArena;
        this.arenas = arenas;
        this.systemBytes = systemBytes;
        this.freeBytes = freeBytes;
        this.subheaps = subheaps;
        this.available = available;
    }

    /** Number of arenas glibc has created. Capped at 8x cores, or by MALLOC_ARENA_MAX. */
    public int arenas() {
        return this.arenas;
    }

    /** Bytes glibc holds from the OS across all arenas - this is what shows up in RSS. */
    public long systemBytes() {
        return this.systemBytes;
    }

    /** Bytes free inside those arenas: allocated from the OS, not in use, not returned. */
    public long freeBytes() {
        return this.freeBytes;
    }

    /** Total 64 MiB subheaps chained across all arenas. Arena COUNT is capped; this is not. */
    public int subheaps() {
        return this.subheaps;
    }

    /**
     * Per-arena figures, keyed by glibc's arena number.
     *
     * <p>This is the closest thing to attribution available without hooking malloc. glibc binds a
     * thread to an arena on first allocation and keeps it there, so growth concentrated in one
     * arena means the leak belongs to the small set of threads bound to that arena rather than to
     * the process at large. It does not name the caller, but it turns "something in this JVM" into
     * "one of these few threads", which is a question a thread dump can then answer.</p>
     */
    public Map<Integer, long[]> perArena() {
        // Unmodifiable: this returned the live map, and the long[] values inside it are mutable
        // regardless, so a caller could silently corrupt a captured measurement.
        return java.util.Collections.unmodifiableMap(this.perArena);
    }

    /**
     * Bytes glibc currently holds in mmap-served allocations.
     *
     * <p>Requests above the mmap threshold bypass the arenas entirely, so they appear in none of
     * the per-heap figures. Reporting arena totals alone therefore understates what the allocator
     * is holding, and on a process whose leak consists of large blocks it would understate it by
     * the entire amount that matters.</p>
     */
    public long mmapBytes() {
        return this.mmapBytes;
    }

    public long mmapCount() {
        return this.mmapCount;
    }

    /** Arena-held bytes plus mmap-served bytes: everything glibc holds. */
    public long totalHeldBytes() {
        return this.systemBytes + this.mmapBytes;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /** Fraction of glibc's held memory that is free - the fragmentation signal. */
    /** Free fraction of ARENA memory only. mmap-served blocks have no free concept. */
    public double arenaFreeRatio() {
        return this.systemBytes == 0 ? 0 : (double) this.freeBytes / this.systemBytes;
    }

    /**
     * Free fraction of everything glibc holds, arenas and mmap together.
     *
     * <p>This is the ratio the fragmentation-versus-leak decision must use. mmap-served blocks are
     * returned to the OS the moment they are freed, so any that are still held are live by
     * definition. Measuring the free fraction against arena bytes alone, while presenting the
     * total as what is held, compares a numerator and denominator that describe different things -
     * and on a process whose growth is mostly mmap it would report comfortable fragmentation
     * where the memory is in fact entirely live.</p>
     */
    public double freeRatio() {
        long total = totalHeldBytes();
        return total == 0 ? 0 : (double) this.freeBytes / total;
    }

    /**
     * Reads current arena statistics, or an unavailable instance on a non-glibc platform or a
     * JVM without the diagnostic command.
     */
    public static GlibcArenaInfo capture() {
        String xml = DiagnosticCommand.execute("System.native_heap_info");
        if (xml.startsWith(DiagnosticCommand.UNAVAILABLE) || !xml.contains("<malloc")) {
            return new GlibcArenaInfo(0, 0, 0, 0, false, java.util.Collections.emptyMap(), 0, 0);
        }

        int arenas = 0;
        long system = 0;
        long free = 0;
        int subheaps = 0;

        Map<Integer, long[]> perArena = new java.util.LinkedHashMap<>();
        Matcher heaps = HEAP.matcher(xml);
        while (heaps.find()) {
            int nr = Integer.parseInt(heaps.group(1));
            String body = heaps.group(2);
            arenas++;
            // Parsed once each; these were previously evaluated twice per heap for the same values.
            long aSys = firstLong(SYSTEM_CURRENT, body);
            long aFree = firstLong(REST, body) + firstLong(FAST, body);
            long sub = firstLong(SUBHEAPS, body);
            sub = sub == 0 ? 1 : sub;

            system += aSys;
            free += aFree;
            subheaps += (int) sub;
            perArena.put(nr, new long[]{aSys, aFree, sub});
        }

        // The top-level totals sit AFTER the last </heap>. Parsing them from the whole document
        // would match the first per-heap element instead, so the heap blocks are removed first.
        // Strip heap blocks first: the same element names appear inside them, and matching against
        // the whole document would take the first per-heap value instead of the top-level total.
        String topLevel = HEAP_BLOCK.matcher(xml).replaceAll("");
        long mmapBytes = 0;
        long mmapCount = 0;
        Matcher mmap = MMAP_TOTAL.matcher(topLevel);
        if (mmap.find()) {
            mmapCount = Long.parseLong(mmap.group(1));
            mmapBytes = Long.parseLong(mmap.group(2));
        }

        return new GlibcArenaInfo(arenas, system, free, subheaps, arenas > 0, perArena, mmapBytes, mmapCount);
    }

    private static long firstLong(Pattern pattern, String text) {
        Matcher m = pattern.matcher(text);
        return m.find() ? Long.parseLong(m.group(1)) : 0L;
    }
}
