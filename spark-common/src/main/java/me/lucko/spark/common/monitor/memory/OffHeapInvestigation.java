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
        final Map<String, Long> mappings; // address range -> resident bytes

        Sample(long timestamp, ProcessMemorySnapshot process, GlibcArenaInfo arenas, Map<String, Long> mappings) {
            this.timestamp = timestamp;
            this.process = process;
            this.arenas = arenas;
            this.mappings = mappings;
        }
    }

    private final List<Sample> samples = new ArrayList<>();
    private String nmtBaselineResult;

    /**
     * Takes the opening sample and asks NMT to record a baseline.
     *
     * <p>The NMT baseline matters more than it looks: {@code summary.diff} against it is the only
     * way to see which JVM region grew, as opposed to how large each is now. A region that is
     * merely big has usually always been big.</p>
     */
    public void begin() {
        this.nmtBaselineResult = DiagnosticCommand.execute("VM.native_memory", "baseline");
        this.samples.add(capture());
    }

    public void sample() {
        this.samples.add(capture());
    }

    public int sampleCount() {
        return this.samples.size();
    }

    private static Sample capture() {
        Map<String, Long> mappings = new HashMap<>();
        for (ProcessMemory.Mapping m : ProcessMemory.getMappings()) {
            mappings.put(Long.toHexString(m.start()) + "-" + Long.toHexString(m.end()), m.rss());
        }
        return new Sample(System.currentTimeMillis(), ProcessMemorySnapshot.capture(),
                GlibcArenaInfo.capture(), mappings);
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
        out.add(String.format("Duration: %.1f hours, %d samples", hours, this.samples.size()));
        out.add("");
        out.add("--- Growth ---");
        out.add(rate("Resident set size", rssGrowth, hours));
        out.add(rate("  Java heap committed", heapGrowth, hours));
        out.add(rate("  JVM non-heap", nonHeapGrowth, hours));
        out.add(rate("  NIO direct buffers", directGrowth, hours));
        out.add(rate("  glibc arenas (malloc_info)", arenaGrowth, hours));
        out.add(rate("Unaccounted", unaccountedGrowth, hours));
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
        String nmtDiff = DiagnosticCommand.execute("VM.native_memory", "summary.diff", "scale=MB");
        long nmtGrowth = -1;
        out.add("--- NMT categories that changed ---");
        if (nmtDiff.startsWith(DiagnosticCommand.UNAVAILABLE) || nmtDiff.contains("not enabled")) {
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

        out.add("--- Mappings that appeared or grew ---");
        appendMappingDelta(out, first, last);
        out.add("");

        out.add("--- VERDICT ---");
        appendVerdict(out, rssGrowth, unaccountedGrowth, arenaGrowth, nmtGrowth, last, hours);
        return out;
    }

    private void appendMappingDelta(List<String> out, Sample first, Sample last) {
        long newBytes = 0;
        int newCount = 0;
        long grewBytes = 0;
        int grewCount = 0;
        Map<String, Long> growth = new LinkedHashMap<>();

        for (Map.Entry<String, Long> e : last.mappings.entrySet()) {
            Long before = first.mappings.get(e.getKey());
            if (before == null) {
                newBytes += e.getValue();
                newCount++;
                growth.put(e.getKey() + " (new)", e.getValue());
            } else if (e.getValue() > before) {
                grewBytes += e.getValue() - before;
                grewCount++;
                growth.put(e.getKey(), e.getValue() - before);
            }
        }

        out.add(String.format("  %d new mappings holding %s", newCount, FormatUtil.formatBytes(newBytes)));
        out.add(String.format("  %d existing mappings grew by %s", grewCount, FormatUtil.formatBytes(grewBytes)));

        // The SHAPE of the growth is the lead when nothing else attributes it. 64 MiB regions are
        // glibc arena subheaps; a handful of very large ones is a direct-buffer or JNI consumer;
        // thousands of small ones is thread stacks.
        int sixtyFour = 0;
        for (Map.Entry<String, Long> e : growth.entrySet()) {
            if (e.getValue() >= 60L * 1024 * 1024 && e.getValue() <= 68L * 1024 * 1024) {
                sixtyFour++;
            }
        }
        if (sixtyFour > 0) {
            out.add(String.format("  of which %d are 64 MiB - the glibc arena subheap signature", sixtyFour));
        }

        growth.entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<String, Long> e) -> e.getValue()).reversed())
                .limit(15)
                .forEach(e -> out.add(String.format("    %12s  %s", FormatUtil.formatBytes(e.getValue()), e.getKey())));
    }

    private void appendVerdict(List<String> out, long rssGrowth, long unaccountedGrowth,
                               long arenaGrowth, long nmtGrowth, Sample last, double hours) {
        if (rssGrowth < 256L * 1024 * 1024) {
            out.add("  No meaningful growth over this window. Either there is no leak, or the");
            out.add("  window was too short or too quiet. Re-run for longer, under load.");
            return;
        }

        out.add(String.format("  Growing at %s/hour.", FormatUtil.formatBytes((long) (rssGrowth / hours))));

        boolean nmtExplains = nmtGrowth > 0 && nmtGrowth * 2 > rssGrowth;
        boolean glibcExplains = arenaGrowth * 2 > unaccountedGrowth && arenaGrowth > 0;

        if (nmtExplains) {
            out.add("  NMT accounts for most of it: this is the JVM itself, not a plugin.");
            out.add("  The category listed above names the subsystem. GC growth usually means");
            out.add("  collector metadata scaling with heap size - try -XX:SoftMaxHeapSize to");
            out.add("  confirm, since that metadata scales with the heap.");
        } else if (glibcExplains && last.arenas.freeRatio() > 0.4) {
            out.add("  glibc holds it and most is FREE: allocator fragmentation, not a leak.");
            out.add("  Allocations were released; glibc never returned the pages. Cap arenas");
            out.add("  with MALLOC_ARENA_MAX=2, and reclaim now with --trim.");
        } else if (glibcExplains) {
            out.add("  glibc holds it and it is LIVE: something calls malloc and never frees.");
            out.add("  NMT does not see it, so the caller is native code outside the JVM -");
            out.add("  a JNI library, or a JVM path that allocates untagged.");
            out.add("  Next: /spark profiler start --leaks --timeout 3600 --thread *");
            out.add("  If that attributes far less than the rate above, the profiler is");
            out.add("  sampling past it and only a uprobe on libc malloc will attribute it.");
        } else {
            out.add("  Neither NMT nor glibc accounts for it: the memory arrived by mmap, not");
            out.add("  malloc. Both the leak profiler and malloc_info are blind to that by");
            out.add("  construction. The mapping sizes above are the lead - match them against");
            out.add("  loaded native libraries in --maps.");
        }
    }

    private static long parseNmtTotalDelta(String diff) {
        for (String line : diff.split("\n")) {
            if (line.contains("Total:") && line.contains("committed=")) {
                int i = line.indexOf("committed=");
                String rest = line.substring(i + 10);
                StringBuilder digits = new StringBuilder();
                for (int c = 0; c < rest.length(); c++) {
                    char ch = rest.charAt(c);
                    if (Character.isDigit(ch)) {
                        digits.append(ch);
                    } else {
                        break;
                    }
                }
                if (digits.length() > 0) {
                    return Long.parseLong(digits.toString()) * 1024 * 1024;
                }
            }
        }
        return -1;
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
