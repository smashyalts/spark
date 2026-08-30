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
import me.lucko.spark.common.util.FormatUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * fork - a timed, correlated off-heap investigation.
 *
 * <p>Every individual measurement needed to locate an off-heap leak is already available in
 * process, and none of them identifies one alone. Resident size says memory grew. NMT says how
 * much of it the JVM admits to. glibc's malloc_info says how much the allocator is holding and
 * how much of that is free. smaps says which address ranges appeared or expanded. A leak is
 * found in the RELATIONSHIPS between those four, sampled over a long enough window that the
 * growth is unambiguous - and reconstructing that correlation by hand, from separate commands
 * run at different moments, is slow and error-prone in exactly the way that lets a leak survive
 * days of investigation.</p>
 *
 * <p>The decision tree this automates:</p>
 * <ul>
 *   <li>NMT's own growth accounts for the rise -&gt; the JVM is the consumer, and the category
 *       that grew names the subsystem. No plugin is involved.</li>
 *   <li>glibc holds the rise but NMT does not -&gt; native code outside the JVM is calling
 *       malloc. The free ratio then separates fragmentation from genuine retention.</li>
 *   <li>Neither accounts for it -&gt; the memory arrived by mmap rather than malloc, and the
 *       new mapping addresses and sizes are the only remaining lead.</li>
 * </ul>
 *
 * <p>Deliberately biased toward long runs. Short windows produced most of the wrong answers this
 * was written to prevent: a startup ramp reads as a leak, a GC cycle reads as a plateau, and a
 * bursty allocator looks idle between bursts.</p>
 */
public final class OffHeapInvestigation {

    /** One sample: every source of truth, captured together so they can be compared. */
    public static final class Sample {
        final long timestamp;
        final ProcessMemorySnapshot process;
        final GlibcArenaInfo arenas;
        // Keyed by START address, not by the full range. Mappings grow upward, so the end address
        // moves while the start stays put - keying by the range made every grown mapping look like
        // a brand new one, and counted its whole size as growth instead of its delta.
        final Map<Long, long[]> mappings; // start address -> {resident bytes, mapped size}

        // Leak classes that leave almost no trace in RSS, and so would be missed entirely by a
        // memory-only investigation: descriptors exhaust a ulimit, classloaders accumulate through
        // plugin reloads, threads pile up holding stacks. Each kills a server in its own way.
        final long openFds;
        final long loadedClasses;
        final long unloadedClasses;
        final int threads;
        final long gcCount;
        final long gcTimeMs;

        Sample(long timestamp, ProcessMemorySnapshot process, GlibcArenaInfo arenas, Map<Long, long[]> mappings,
               long openFds, long loadedClasses, long unloadedClasses, int threads, long gcCount, long gcTimeMs) {
            this.timestamp = timestamp;
            this.process = process;
            this.arenas = arenas;
            this.mappings = mappings;
            this.openFds = openFds;
            this.loadedClasses = loadedClasses;
            this.unloadedClasses = unloadedClasses;
            this.threads = threads;
            this.gcCount = gcCount;
            this.gcTimeMs = gcTimeMs;
        }
    }

    // Copy-on-write: sampled from the investigation thread, read by command threads asking for
    // progress. Sample counts are tiny and reads are rare, so the copy cost is irrelevant.
    private final List<Sample> samples = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile boolean nmtBaselineOk;
    private java.nio.file.Path incrementalLog;

    /**
     * Takes the opening sample and asks NMT to record a baseline.
     *
     * <p>The NMT baseline matters more than it looks: {@code summary.diff} against it is the only
     * way to see which JVM region grew, as opposed to how large each is now. A region that is
     * merely big has usually always been big.</p>
     */
    /**
     * @param incrementalLog file to append each sample to, or null. Written as the run proceeds so
     *                       a crash mid-investigation still leaves the data behind - which matters
     *                       most on exactly the servers worth investigating, since an OOM during a
     *                       two hour run would otherwise destroy the evidence it was gathering.
     */
    public void begin(java.nio.file.Path incrementalLog) {
        this.incrementalLog = incrementalLog;
        String result = DiagnosticCommand.execute("VM.native_memory", "baseline");
        // A failed baseline is not cosmetic: summary.diff would then compare against whatever
        // baseline was set previously, silently reporting the wrong deltas.
        this.nmtBaselineOk = !result.startsWith(DiagnosticCommand.UNAVAILABLE)
                && !result.contains("not enabled");
        record(capture());
    }

    public void sample() {
        record(capture());
    }

    private void record(Sample sample) {
        this.samples.add(sample);
        appendIncremental(sample);
    }

    private void appendIncremental(Sample sample) {
        if (this.incrementalLog == null) {
            return;
        }
        try {
            StringBuilder sb = new StringBuilder();
            if (this.samples.size() == 1) {
                sb.append("time,rss,heap_used,heap_committed,non_heap,nio_direct,")
                        .append("glibc_held,glibc_free,arenas,subheaps,unaccounted,")
                        .append("open_fds,loaded_classes,unloaded_classes,threads,gc_count,gc_time_ms\n");
            }
            sb.append(sample.timestamp).append(',')
                    .append(sample.process.rss()).append(',')
                    .append(sample.process.heapUsed()).append(',')
                    .append(sample.process.heapCommitted()).append(',')
                    .append(sample.process.nonHeapCommitted()).append(',')
                    .append(sample.process.directUsed()).append(',')
                    .append(sample.arenas.systemBytes()).append(',')
                    .append(sample.arenas.freeBytes()).append(',')
                    .append(sample.arenas.arenas()).append(',')
                    .append(sample.arenas.subheaps()).append(',')
                    .append(sample.process.unaccounted()).append(',')
                    .append(sample.openFds).append(',')
                    .append(sample.loadedClasses).append(',')
                    .append(sample.unloadedClasses).append(',')
                    .append(sample.threads).append(',')
                    .append(sample.gcCount).append(',')
                    .append(sample.gcTimeMs).append('\n');
            java.nio.file.Files.write(this.incrementalLog,
                    sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (Throwable t) {
            // best effort - never let logging break the investigation
        }
    }

    /** Progress line for an investigation already in flight. */
    public String progress() {
        if (this.samples.isEmpty()) {
            return "starting up";
        }
        Sample first = this.samples.get(0);
        Sample last = this.samples.get(this.samples.size() - 1);
        long mins = (last.timestamp - first.timestamp) / 60000;
        return String.format("%d samples over %d minutes, RSS %s -> %s, unaccounted %s -> %s",
                this.samples.size(), mins,
                FormatUtil.formatBytes(first.process.rss()), FormatUtil.formatBytes(last.process.rss()),
                FormatUtil.formatBytes(first.process.unaccounted()),
                FormatUtil.formatBytes(last.process.unaccounted()));
    }

    public int sampleCount() {
        return this.samples.size();
    }

    private static Sample capture() {
        Map<Long, long[]> mappings = new HashMap<>();
        for (ProcessMemory.Mapping m : ProcessMemory.getMappings()) {
            mappings.put(m.start(), new long[]{m.rss(), m.size()});
        }
        java.lang.management.ClassLoadingMXBean classes = java.lang.management.ManagementFactory.getClassLoadingMXBean();
        long gcCount = 0;
        long gcTime = 0;
        for (java.lang.management.GarbageCollectorMXBean gc :
                java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getCollectionCount() > 0) {
                gcCount += gc.getCollectionCount();
            }
            if (gc.getCollectionTime() > 0) {
                gcTime += gc.getCollectionTime();
            }
        }
        return new Sample(System.currentTimeMillis(), ProcessMemorySnapshot.capture(),
                GlibcArenaInfo.capture(), mappings,
                ProcessMemory.getOpenFileDescriptors(),
                classes.getLoadedClassCount(), classes.getUnloadedClassCount(),
                java.lang.management.ManagementFactory.getThreadMXBean().getThreadCount(),
                gcCount, gcTime);
    }

    /** Renders the full report. Returns the lines; the caller decides where they go. */
    public List<String> report() {
        List<String> out = new ArrayList<>();
        if (this.samples.size() < 2) {
            out.add("Not enough samples to draw a conclusion.");
            return out;
        }

        Sample first = this.samples.get(0);
        Sample last = this.samples.get(this.samples.size() - 1);
        double hours = (last.timestamp - first.timestamp) / 3_600_000d;
        if (hours <= 0) {
            hours = 1 / 3600d;
        }

        long rssGrowth = last.process.rss() - first.process.rss();
        long heapGrowth = last.process.heapCommitted() - first.process.heapCommitted();
        long nonHeapGrowth = last.process.nonHeapCommitted() - first.process.nonHeapCommitted();
        long directGrowth = last.process.directUsed() - first.process.directUsed();
        long arenaGrowth = last.arenas.systemBytes() - first.arenas.systemBytes();
        long unaccountedGrowth = last.process.unaccounted() - first.process.unaccounted();

        out.add("=== OFF-HEAP INVESTIGATION ===");
        out.add(String.format("Duration: %.2f hours, %d samples", hours, this.samples.size()));
        if (windowTooShort(hours)) {
            out.add("WARNING: window under 15 minutes. Hourly rates below are extrapolated from");
            out.add("very little data and a startup ramp or a single GC cycle will dominate them.");
        }
        out.add("");
        out.add("--- Growth ---");
        out.add(rate("Resident set size", rssGrowth, hours));
        out.add(rate("  Java heap committed", heapGrowth, hours));
        out.add(rate("  JVM non-heap", nonHeapGrowth, hours));
        out.add(rate("  NIO direct buffers", directGrowth, hours));
        out.add(rate("  glibc arenas (malloc_info)", arenaGrowth, hours));
        out.add(rate("Unaccounted", unaccountedGrowth, hours));
        out.add("");

        // A Java object leak shows here and nowhere else in this report: committed heap climbing
        // while used-after-GC never returns to its earlier floor. Without this the command would
        // answer "not a native leak" and leave the most common kind of leak unexamined.
        out.add("--- Java heap trend ---");
        // Compare the post-GC FLOOR of the first half against the second half. An earlier version
        // compared the overall minimum against the first sample, which is unsatisfiable by
        // construction - the overall minimum includes the first sample, so it can never exceed it,
        // and the Java-leak branch could never fire. Halves are the correct comparison: a leak
        // raises the floor the collector can reach as the run progresses.
        int half = this.samples.size() / 2;
        long firstHalfFloor = Long.MAX_VALUE;
        long secondHalfFloor = Long.MAX_VALUE;
        long peak = 0;
        for (int i = 0; i < this.samples.size(); i++) {
            long used = this.samples.get(i).process.heapUsed();
            peak = Math.max(peak, used);
            if (i < half) {
                firstHalfFloor = Math.min(firstHalfFloor, used);
            } else {
                secondHalfFloor = Math.min(secondHalfFloor, used);
            }
        }
        long firstUsed = first.process.heapUsed();
        long lastUsed = last.process.heapUsed();
        out.add(String.format("  used: first %s, last %s, peak %s",
                FormatUtil.formatBytes(firstUsed), FormatUtil.formatBytes(lastUsed),
                FormatUtil.formatBytes(peak)));
        out.add(String.format("  post-GC floor: first half %s -> second half %s",
                FormatUtil.formatBytes(firstHalfFloor), FormatUtil.formatBytes(secondHalfFloor)));
        out.add(String.format("  committed: %s -> %s", FormatUtil.formatBytes(first.process.heapCommitted()),
                FormatUtil.formatBytes(last.process.heapCommitted())));
        long floorRise = secondHalfFloor - firstHalfFloor;
        boolean javaHeapLeak = this.samples.size() >= 4 && half > 0
                && firstHalfFloor != Long.MAX_VALUE && secondHalfFloor != Long.MAX_VALUE
                && floorRise > 256L * 1024 * 1024;
        if (javaHeapLeak) {
            out.add("  The post-GC FLOOR rose by " + FormatUtil.formatBytes(floorRise)
                    + " - that is a Java object leak.");
            out.add("  Capture: /spark profiler start --heap-leaks --timeout 1800 --thread *");
        } else {
            out.add("  Floor is stable - no Java object leak evident in this window.");
        }
        out.add("");

        // These four cost almost nothing in RSS and so are invisible to every other section here,
        // yet each one takes a server down on its own schedule.
        out.add("--- Other leak classes ---");
        long fdLimit = ProcessMemory.getFileDescriptorLimit();
        out.add(String.format("  open file descriptors: %d -> %d%s", first.openFds, last.openFds,
                fdLimit > 0 ? " (limit " + fdLimit + ")" : ""));
        out.add(String.format("  live classes: %d -> %d (unloaded %d -> %d)",
                first.loadedClasses, last.loadedClasses, first.unloadedClasses, last.unloadedClasses));
        out.add(String.format("  threads: %d -> %d", first.threads, last.threads));
        out.add(String.format("  GC collections: %d over this window, %d ms total",
                last.gcCount - first.gcCount, last.gcTimeMs - first.gcTimeMs));

        long fdGrowth = last.openFds - first.openFds;
        long classGrowth = last.loadedClasses - first.loadedClasses;
        int threadGrowth = last.threads - first.threads;
        boolean fdLeak = first.openFds > 0 && fdGrowth > 200;
        boolean classLeak = classGrowth > 5000;
        boolean threadLeak = threadGrowth > 100;

        if (fdLeak) {
            out.add(String.format("  FILE DESCRIPTOR LEAK: +%d over %.1f hours (%.0f/hour).",
                    fdGrowth, hours, fdGrowth / hours));
            if (fdLimit > 0) {
                out.add(String.format("  At this rate the %d limit is reached in about %.0f hours.",
                        fdLimit, (fdLimit - last.openFds) / Math.max(1.0, fdGrowth / hours)));
            }
            out.add("  Something opens files or sockets without closing them.");
        }
        if (classLeak) {
            out.add(String.format("  CLASSLOADER LEAK: live classes +%d. Repeated plugin reloads that",
                    classGrowth));
            out.add("  leave old classloaders reachable will do this, and it grows metaspace, not heap.");
        }
        if (threadLeak) {
            out.add(String.format("  THREAD LEAK: +%d threads. Each holds a stack; check for plugins",
                    threadGrowth));
            out.add("  spawning raw threads instead of using a pool.");
        }
        if (!fdLeak && !classLeak && !threadLeak) {
            out.add("  No descriptor, classloader or thread leak evident.");
        }
        out.add("");

        out.add("--- glibc allocator, start vs end ---");
        out.add(String.format("  arenas   %d -> %d", first.arenas.arenas(), last.arenas.arenas()));
        out.add(String.format("  subheaps %d -> %d", first.arenas.subheaps(), last.arenas.subheaps()));
        out.add(String.format("  held     %s -> %s", FormatUtil.formatBytes(first.arenas.systemBytes()),
                FormatUtil.formatBytes(last.arenas.systemBytes())));
        out.add(String.format("  free     %.0f%% -> %.0f%%", first.arenas.freeRatio() * 100,
                last.arenas.freeRatio() * 100));
        out.add("");

        // NMT diff is the discriminator between "the JVM did this" and "something else did".
        String nmtDiff = this.nmtBaselineOk
                ? DiagnosticCommand.execute("VM.native_memory", "summary.diff", "scale=MB") : "";
        long nmtGrowth = -1;
        out.add("--- NMT categories that changed ---");
        if (!this.nmtBaselineOk) {
            out.add("  NMT baseline was not set at the start of this run, so a diff here would");
            out.add("  compare against an older baseline and report the wrong deltas. Skipped.");
            nmtDiff = "";
        }
        if (nmtDiff.isEmpty() || nmtDiff.startsWith(DiagnosticCommand.UNAVAILABLE) || nmtDiff.contains("not enabled")) {
            out.add("  NMT unavailable. Add -XX:NativeMemoryTracking=summary and restart -");
            out.add("  without it there is no way to tell JVM-internal growth from external.");
        } else {
            nmtGrowth = parseNmtTotalDelta(nmtDiff);
            for (String line : nmtDiff.split("\n")) {
                if (line.contains("+") && (line.trim().startsWith("-") || line.contains("Total:"))) {
                    out.add("  " + line.trim());
                }
            }
            if (nmtGrowth >= 0) {
                out.add(String.format("  NMT total committed delta: %s", FormatUtil.formatBytes(nmtGrowth)));
            }
        }
        out.add("");

        // Per-arena growth is the only attribution available without hooking malloc. glibc binds
        // a thread to an arena and keeps it there, so concentrated growth means a small set of
        // threads, not the whole process.
        out.add("--- Per-arena growth (glibc binds threads to arenas) ---");
        Map<Integer, long[]> before = first.arenas.perArena();
        Map<Integer, long[]> after = last.arenas.perArena();
        List<int[]> ranked = new ArrayList<>();
        for (Map.Entry<Integer, long[]> e : after.entrySet()) {
            long[] b = before.get(e.getKey());
            long grew = e.getValue()[0] - (b == null ? 0 : b[0]);
            ranked.add(new int[]{e.getKey(), (int) (grew / (1024 * 1024))});
        }
        ranked.sort((x, y) -> Integer.compare(y[1], x[1]));
        long totalArenaGrowthMb = 0;
        for (int[] r : ranked) {
            totalArenaGrowthMb += Math.max(0, r[1]);
        }
        for (int i = 0; i < Math.min(8, ranked.size()); i++) {
            int nr = ranked.get(i)[0];
            int mb = ranked.get(i)[1];
            long[] cur = after.get(nr);
            double pct = totalArenaGrowthMb > 0 ? (100.0 * Math.max(0, mb) / totalArenaGrowthMb) : 0;
            out.add(String.format("  arena %-3d  +%-8s (%4.0f%% of growth)  now %s, %.0f%% free, %d subheaps",
                    nr, FormatUtil.formatBytes(mb * 1024L * 1024L), pct,
                    FormatUtil.formatBytes(cur[0]),
                    cur[0] > 0 ? (100.0 * cur[1] / cur[0]) : 0, cur[2]));
        }
        if (!ranked.isEmpty() && totalArenaGrowthMb > 0) {
            double topShare = 100.0 * Math.max(0, ranked.get(0)[1]) / totalArenaGrowthMb;
            if (topShare > 60) {
                out.add(String.format("  Growth is CONCENTRATED in arena %d (%.0f%%). The leak belongs to the",
                        ranked.get(0)[0], topShare));
                out.add("  few threads bound to that arena, not to the process at large.");
            } else {
                out.add("  Growth is SPREAD across arenas - many threads allocate and retain, which");
                out.add("  points at a shared subsystem (GC, chunk system) rather than one plugin.");
            }
        }
        out.add("");

        out.add("--- Mappings that appeared or grew ---");
        appendMappingDelta(out, first, last);
        out.add("");

        out.add("--- VERDICT ---");
        appendVerdict(out, rssGrowth, unaccountedGrowth, arenaGrowth, nmtGrowth, heapGrowth,
                javaHeapLeak, floorRise, last, hours,
                fdLeak ? fdGrowth : 0, classLeak ? classGrowth : 0, threadLeak ? threadGrowth : 0);
        return out;
    }

    private void appendMappingDelta(List<String> out, Sample first, Sample last) {
        long newBytes = 0;
        int newCount = 0;
        long grewBytes = 0;
        int grewCount = 0;
        long goneBytes = 0;
        int goneCount = 0;
        int arenaSized = 0;

        // value: bytes gained. key: address, plus a marker for mappings that did not exist before.
        Map<String, Long> growth = new LinkedHashMap<>();

        for (Map.Entry<Long, long[]> e : last.mappings.entrySet()) {
            long[] now = e.getValue();
            long[] before = first.mappings.get(e.getKey());
            long gained;
            String label;
            if (before == null) {
                gained = now[0];
                newBytes += gained;
                newCount++;
                label = Long.toHexString(e.getKey()) + " (new, " + FormatUtil.formatBytes(now[1]) + " mapped)";
            } else if (now[0] > before[0]) {
                gained = now[0] - before[0];
                grewBytes += gained;
                grewCount++;
                label = Long.toHexString(e.getKey()) + " (grew, " + FormatUtil.formatBytes(now[1]) + " mapped)";
            } else {
                continue;
            }
            // The arena signature is the MAPPED SIZE being 64 MiB, not the amount gained. A region
            // that merely grew by 64 MiB is not an arena subheap, and counting it as one pointed
            // at glibc for growth that had nothing to do with it.
            if (now[1] >= 60L * 1024 * 1024 && now[1] <= 68L * 1024 * 1024) {
                arenaSized++;
            }
            growth.put(label, gained);
        }

        // Mappings that disappeared offset the gains. Without them the report can claim more
        // growth than the process actually gained, which undermines every figure beside it.
        for (Map.Entry<Long, long[]> e : first.mappings.entrySet()) {
            if (!last.mappings.containsKey(e.getKey())) {
                goneBytes += e.getValue()[0];
                goneCount++;
            }
        }

        out.add(String.format("  %d new mappings holding %s", newCount, FormatUtil.formatBytes(newBytes)));
        out.add(String.format("  %d existing mappings grew by %s", grewCount, FormatUtil.formatBytes(grewBytes)));
        out.add(String.format("  %d mappings disappeared, releasing %s", goneCount, FormatUtil.formatBytes(goneBytes)));
        out.add(String.format("  net from mappings: %s%s",
                (newBytes + grewBytes - goneBytes) < 0 ? "-" : "+",
                FormatUtil.formatBytes(Math.abs(newBytes + grewBytes - goneBytes))));
        if (arenaSized > 0) {
            out.add(String.format("  %d of the growing mappings are 64 MiB - the glibc arena subheap signature",
                    arenaSized));
        }

        growth.entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<String, Long> e) -> e.getValue()).reversed())
                .limit(15)
                .forEach(e -> out.add(String.format("    %12s  %s", FormatUtil.formatBytes(e.getValue()), e.getKey())));
    }

    private void appendVerdict(List<String> out, long rssGrowth, long unaccountedGrowth,
                               long arenaGrowth, long nmtGrowth, long heapGrowth,
                               boolean javaHeapLeak, long floorRise, Sample last, double hours,
                               long fdGrowth, long classGrowth, int threadGrowth) {
        double perHour = rssGrowth / hours;

        // Rate, not absolute size. A fixed byte threshold calls a slow leak "no growth" over a
        // short window and calls normal warm-up a leak over a long one.
        boolean otherLeak = fdGrowth > 0 || classGrowth > 0 || threadGrowth > 0;
        if (perHour < 64L * 1024 * 1024 && !javaHeapLeak && !otherLeak) {
            out.add(String.format("  Growing at only %s/hour - no meaningful leak in this window.",
                    FormatUtil.formatBytes((long) Math.max(0, perHour))));
            out.add("  Either there is nothing wrong, or the window was too quiet. Re-run under load.");
            return;
        }

        out.add(String.format("  Growing at %s/hour.", FormatUtil.formatBytes((long) perHour)));

        // Reported first and unconditionally: a rising post-GC floor is the most actionable
        // finding available, and only the verdict is echoed to chat - a Java leak buried in the
        // trend section above would never be seen by anyone who did not open the file.
        if (fdGrowth > 0) {
            out.add("  FILE DESCRIPTOR LEAK: +" + fdGrowth + " open descriptors. This kills the");
            out.add("  server at the ulimit regardless of how much memory is free.");
        }
        if (classGrowth > 0) {
            out.add("  CLASSLOADER LEAK: +" + classGrowth + " live classes - metaspace will exhaust.");
        }
        if (threadGrowth > 0) {
            out.add("  THREAD LEAK: +" + threadGrowth + " threads, each holding a stack.");
        }
        if (javaHeapLeak) {
            out.add("  JAVA OBJECT LEAK: the post-GC floor rose by " + FormatUtil.formatBytes(floorRise) + ".");
            out.add("  Objects are being retained and the collector cannot reclaim them. This is a");
            out.add("  plugin holding references - the most common leak and the easiest to fix.");
            out.add("  Next: /spark profiler start --heap-leaks --timeout 1800 --thread *");
            out.add("");
        }

        // NMT's total includes Java heap commitment, which grows for entirely healthy reasons.
        // Comparing the raw total against RSS growth lets an expanding heap mask a native leak
        // underneath it, so the heap is subtracted before asking whether the JVM explains things.
        long nmtNonHeap = nmtGrowth >= 0 ? nmtGrowth - Math.max(0, heapGrowth) : -1;
        boolean nmtExplains = nmtNonHeap > 0 && unaccountedGrowth > 0 && nmtNonHeap * 2 > unaccountedGrowth;
        boolean glibcExplains = arenaGrowth > 0 && unaccountedGrowth > 0
                && arenaGrowth * 2 > unaccountedGrowth;

        if (unaccountedGrowth <= 0) {
            out.add("  All of the growth is accounted for by the JVM's own regions - heap, code,");
            out.add("  metaspace or direct buffers. Nothing is leaking outside the JVM's view.");
            if (heapGrowth > 0) {
                out.add("  Java heap committed grew " + FormatUtil.formatBytes(heapGrowth)
                        + "; that is normal expansion toward -Xmx unless the floor is rising too.");
            }
        } else if (nmtExplains) {
            out.add("  NMT accounts for the off-heap growth: this is the JVM itself, not a plugin.");
            out.add("  The category listed above names the subsystem. GC growth usually means");
            out.add("  collector metadata scaling with heap size - test with -XX:SoftMaxHeapSize,");
            out.add("  since that metadata scales with the heap and should shrink with it.");
        } else if (glibcExplains && last.arenas.freeRatio() > 0.4) {
            out.add("  glibc holds it and most is FREE: allocator fragmentation, not a leak.");
            out.add("  Allocations were released; glibc never returned the pages. Cap arenas with");
            out.add("  MALLOC_ARENA_MAX=2, and reclaim what is already free with --trim.");
        } else if (glibcExplains) {
            out.add("  glibc holds it and it is LIVE: something calls malloc and never frees.");
            out.add("  NMT does not see it, so the caller is native code outside the JVM - a JNI");
            out.add("  library, or a JVM path that allocates untagged.");
            out.add("  Check the per-arena breakdown above: growth concentrated in one arena means");
            out.add("  a small set of threads, which /spark offheap --jcmd Thread.print can name.");
            out.add("  Next: /spark profiler start --leaks --timeout 3600 --thread *");
        } else {
            out.add("  Neither NMT nor glibc accounts for it: the memory arrived by mmap, not");
            out.add("  malloc. Both the leak profiler and malloc_info are blind to that by");
            out.add("  construction. The mapping sizes above are the lead - match them against");
            out.add("  the loaded native libraries in --maps.");
        }
    }

    /**
     * Extracts the committed DELTA from an NMT summary.diff total line.
     *
     * <p>The line reads {@code Total: reserved=3419MB -1MB, committed=1703MB +1607MB}. An earlier
     * version read the digits immediately after {@code committed=}, which is the absolute figure,
     * not the change - so this returned 1703 where the growth was 1607. That made the verdict
     * conclude "the JVM accounts for this" almost unconditionally, since a total committed size
     * will nearly always exceed half of any growth measured over a window.</p>
     */
    static long parseNmtTotalDelta(String diff) {
        for (String line : diff.split("\n")) {
            if (!line.contains("Total:") || !line.contains("committed=")) {
                continue;
            }
            int i = line.indexOf("committed=");
            String rest = line.substring(i + "committed=".length());
            // skip the absolute value ("1703MB"), then read the signed delta that follows
            java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("^\\d+[KMG]?B\\s+([+-])(\\d+)([KMG]?)B")
                    .matcher(rest.trim());
            if (m.find()) {
                long value = Long.parseLong(m.group(2));
                String unit = m.group(3);
                if ("K".equals(unit)) {
                    value *= 1024L;
                } else if ("M".equals(unit)) {
                    value *= 1024L * 1024L;
                } else if ("G".equals(unit)) {
                    value *= 1024L * 1024L * 1024L;
                }
                return "-".equals(m.group(1)) ? -value : value;
            }
            return 0; // total line present but no delta shown - nothing changed
        }
        return -1;
    }

    /** True when the window is too short for an hourly rate to mean anything. */
    static boolean windowTooShort(double hours) {
        return hours < 0.25;
    }

    private static String rate(String label, long delta, double hours) {
        // '%+s' is invalid - the + flag does not apply to string conversion. Sign is applied
        // to the text instead.
        String sign = delta < 0 ? "-" : "+";
        return String.format("  %-28s %12s   (%s%s/hour)", label,
                sign + FormatUtil.formatBytes(Math.abs(delta)),
                sign, FormatUtil.formatBytes((long) Math.abs(delta / hours)));
    }
}
