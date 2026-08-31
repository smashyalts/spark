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

package me.lucko.spark.common.command.modules;

import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.activitylog.Activity;
import me.lucko.spark.common.command.Arguments;
import me.lucko.spark.common.command.Command;
import me.lucko.spark.common.command.CommandModule;
import me.lucko.spark.common.command.CommandResponseHandler;
import me.lucko.spark.common.command.sender.CommandSender;
import me.lucko.spark.common.command.tabcomplete.TabCompleter;
import me.lucko.spark.common.monitor.DiagnosticCommand;
import me.lucko.spark.common.monitor.MonitoringExecutor;
import me.lucko.spark.common.monitor.memory.GlibcArenaInfo;
import me.lucko.spark.common.monitor.memory.NettyLeakDetector;
import me.lucko.spark.common.monitor.memory.OffHeapInvestigation;
import me.lucko.spark.common.monitor.memory.ProcessMemory;
import me.lucko.spark.common.monitor.memory.ProcessMemorySnapshot;
import me.lucko.spark.common.platform.SparkMetadata;
import me.lucko.spark.common.sampler.async.AsyncSampler;
import me.lucko.spark.common.util.SparkThreadFactory;
import me.lucko.spark.common.util.SparkScheduledThreadPoolExecutor;
import me.lucko.spark.common.util.FormatUtil;
import me.lucko.spark.common.util.MediaTypes;
import me.lucko.spark.proto.SparkSamplerProtos;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerData;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerMetadata;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.event.ClickEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import static net.kyori.adventure.text.Component.text;
import static net.kyori.adventure.text.format.NamedTextColor.DARK_GRAY;
import static net.kyori.adventure.text.format.NamedTextColor.GOLD;
import static net.kyori.adventure.text.format.NamedTextColor.GRAY;
import static net.kyori.adventure.text.format.NamedTextColor.GREEN;
import static net.kyori.adventure.text.format.NamedTextColor.RED;
import static net.kyori.adventure.text.format.NamedTextColor.WHITE;
import static net.kyori.adventure.text.format.NamedTextColor.YELLOW;

/**
 * fork - off-heap memory diagnostics.
 *
 * <p>This is the complement to {@code --leaks}, not a replacement for it. The leak profiler
 * answers "which code path allocated memory that was never freed", but it only sees
 * malloc/realloc/calloc - not mmap - so a leak made of large buffers, or growth in metaspace,
 * the code cache or glibc's arenas, is invisible to it no matter how long the capture runs.
 * This command answers the prior question the profiler cannot: how much memory is unaccounted
 * for at all, and is that amount growing.</p>
 *
 * <p>The distinction it exists to enforce is between a large number and a growing one. A server
 * with 20 GB of unattributable off-heap memory that is stable has a structural cost - GC control
 * structures, arena retention, mapped files - and no bug. The same 20 GB climbing linearly is a
 * leak. One reading cannot tell those apart, which is why {@code --baseline}/{@code --diff} and
 * {@code --watch} are here and why the summary refuses to call anything a leak on its own.</p>
 */
public class NativeMemoryModule implements CommandModule {

    private static final DateTimeFormatter FILE_STAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter ISO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    /** Refused outright: these write huge files, mutate the VM, or stop it for a long time. */
    private static final String[] DESTRUCTIVE_COMMANDS = {
            "GC.heap_dump", "Thread.dump_to_file", "VM.set_flag", "JFR.", "Compiler.directives",
            "System.dump_map", "VM.cds"
    };

    /** Allowed, but warned about: correct and read-only, yet slow enough to be felt. */
    private static final String[] EXPENSIVE_COMMANDS = {
            "GC.class_histogram", "GC.finalizer_info", "GC.run", "VM.classloader_stats",
            // Thread.print brings every thread to a safepoint, which on a regionised server with
            // several hundred threads is a pause the operator should be told about beforehand.
            "Thread.print", "VM.metaspace", "VM.stringtable", "VM.symboltable",
            "VM.systemdictionary", "Compiler.codelist", "Compiler.codecache"
    };

    private static final int DEFAULT_WATCH_MINUTES = 15;
    /** Reports kept per prefix before the oldest are deleted. */
    private static final int MAX_REPORTS_KEPT = 5;
    /** Recording stops rather than filling the disk of an already unhealthy server. */
    private static final long MAX_HISTORY_BYTES = 16L * 1024 * 1024;
    /** Headroom left free when writing a report. */
    private static final long MIN_FREE_DISK_BYTES = 64L * 1024 * 1024;

    // volatile: --baseline and --diff can run from different command threads (console and in
    // game are not the same thread), so an unsynchronised handoff can publish the snapshot
    // half-built or not at all
    /** Retained between invocations so --diff has something to compare against. */
    private volatile ProcessMemorySnapshot baseline;
    // volatile: cleared by cancelWatch() from a command thread AND from the MonitoringExecutor
    // thread, when appendHistory finds the history file has hit its size cap. Read stale, a
    // command thread either cancels a dead task and overwrites a live handle - leaving two tasks
    // appending to one file - or fails to cancel the live one at all.
    private volatile ScheduledFuture<?> watchTask;
    /**
     * Dedicated thread for the investigation.
     *
     * <p>Not MonitoringExecutor: that is a single thread shared with spark's CPU, GC and ping
     * monitors, and an investigation sample parses /proc/self/smaps, which takes hundreds of
     * milliseconds on a large process. Borrowing that thread every interval would stall the
     * statistics spark exists to report, in order to measure memory.</p>
     */
    private volatile ScheduledExecutorService investigationExecutor;

    // volatile: written by the investigation scheduler, read by command threads
    private volatile OffHeapInvestigation investigation;
    private volatile ScheduledFuture<?> investigationTask;

    @Override
    public void registerCommands(Consumer<Command> consumer) {
        consumer.accept(Command.builder()
                .aliases("offheap", "nativememory", "nm")
                .argumentUsage("maps", null)
                .argumentUsage("top", "number")
                .argumentUsage("nmt", null)
                .argumentUsage("nmt-baseline", null)
                .argumentUsage("nmt-diff", null)
                .argumentUsage("trim", null)
                .argumentUsage("flags", null)
                .argumentUsage("baseline", null)
                .argumentUsage("diff", null)
                .argumentUsage("watch", "minutes")
                .argumentUsage("dump", null)
                .argumentUsage("investigate", "minutes")
                .argumentUsage("netty-leak", "level")
                .argumentUsage("diagnose", null)
                .argumentUsage("jcmd", "command")
                .argumentUsage("upload", null)
                .executor(this::execute)
                .tabCompleter((platform, sender, arguments) -> TabCompleter.completeForOpts(arguments,
                        "--maps", "--top", "--nmt", "--nmt-baseline", "--nmt-diff", "--trim",
                        "--flags", "--baseline", "--diff", "--watch", "--dump", "--investigate",
                        "--netty-leak", "--diagnose", "--jcmd", "--upload"))
                .build()
        );
    }

    @Override
    public void close() {
        cancelWatch();
        // Reuse the single implementation rather than repeating it: the copy here omitted clearing
        // the investigation handle, so the two paths did not leave the same state behind.
        cancelInvestigation();
    }

    private void execute(SparkPlatform platform, CommandSender sender, CommandResponseHandler resp, Arguments arguments) {
        // Only the /proc-backed views need Linux. jcmd and NMT go through the platform MBean
        // server and work anywhere, so refusing them here removed working functionality from every
        // non-Linux server for no reason.
        boolean procAvailable = ProcessMemory.getResidentSetSize() >= 0;
        boolean needsProc = !arguments.boolFlag("jcmd")
                && !arguments.boolFlag("nmt") && !arguments.boolFlag("nmt-baseline")
                && !arguments.boolFlag("nmt-diff") && !arguments.boolFlag("flags")
                && !arguments.boolFlag("netty-leak");
        if (!procAvailable && needsProc) {
            resp.replyPrefixed(text("This view needs /proc and is only available on Linux.", RED));
            resp.replyPrefixed(text("--jcmd, --nmt and --flags work on any platform.", GRAY));
            return;
        }

        if (arguments.boolFlag("upload")) {
            upload(platform, sender, resp, arguments);
            return;
        }
        if (arguments.boolFlag("flags")) {
            reportFlags(resp);
            return;
        }
        if (arguments.boolFlag("maps")) {
            reportMappings(resp, arguments.intFlag("top"));
            return;
        }
        if (arguments.boolFlag("nmt") || arguments.boolFlag("nmt-baseline") || arguments.boolFlag("nmt-diff")) {
            reportNativeMemoryTracking(platform, resp, arguments);
            return;
        }
        if (arguments.boolFlag("trim")) {
            trimNativeHeap(resp);
            return;
        }
        if (arguments.boolFlag("baseline")) {
            this.baseline = ProcessMemorySnapshot.capture();
            resp.replyPrefixed(text("Baseline taken. Run --diff later; growth is the signal, size is not.", GOLD));
            return;
        }
        if (arguments.boolFlag("diff")) {
            if (this.baseline == null) {
                resp.replyPrefixed(text("No baseline set. Run '--baseline' first, wait, then '--diff'.", RED));
                return;
            }
            reportDiff(resp, this.baseline, ProcessMemorySnapshot.capture());
            return;
        }
        if (arguments.boolFlag("watch")) {
            // intFlag returns -1 when the flag was given without a value. Defaulting to the
            // documented interval is friendlier than the alternative, where a bare '--watch'
            // silently STOPS recording - the exact opposite of what was typed.
            configureWatch(platform, resp, intFlagOrDefault(arguments, "watch", DEFAULT_WATCH_MINUTES));
            return;
        }
        if (arguments.boolFlag("investigate")) {
            startInvestigation(platform, resp, intFlagOrDefault(arguments, "investigate", 120));
            return;
        }
        if (arguments.boolFlag("netty-leak")) {
            configureNettyLeakDetection(resp, arguments);
            return;
        }
        if (arguments.boolFlag("dump")) {
            writeDump(platform, resp);
            return;
        }
        if (arguments.boolFlag("diagnose")) {
            diagnose(resp);
            return;
        }
        if (arguments.boolFlag("jcmd")) {
            runDiagnosticCommand(platform, resp, arguments);
            return;
        }

        ProcessMemorySnapshot snapshot = ProcessMemorySnapshot.capture();
        reportSummary(resp, snapshot);
        if (this.baseline != null) {
            resp.replyPrefixed(Component.empty());
            reportDiff(resp, this.baseline, snapshot);
        }
    }

    // ------------------------------------------------------------------- reports

    private void reportSummary(CommandResponseHandler resp, ProcessMemorySnapshot s) {
        resp.replyPrefixed(text("Process memory", GOLD));
        resp.replyPrefixed(entry("Resident set size", FormatUtil.formatBytes(s.rss())));
        if (s.swap() > 0) {
            resp.replyPrefixed(entry("Swap", FormatUtil.formatBytes(s.swap())));
        }

        Long cgroupCurrent = s.cgroup().get("memory.current");
        Long inactiveFile = s.cgroup().get("inactive_file");
        if (cgroupCurrent != null) {
            String value = FormatUtil.formatBytes(cgroupCurrent);
            if (inactiveFile != null) {
                value += " (excluding page cache: " + FormatUtil.formatBytes(cgroupCurrent - inactiveFile) + ")";
            }
            resp.replyPrefixed(entry("Container cgroup", value));
            resp.replyPrefixed(text("    hosting panels show this figure, which counts reclaimable page cache", DARK_GRAY));
        }

        resp.replyPrefixed(text("Accounted for by the JVM", GOLD));
        resp.replyPrefixed(entry("Heap (used/committed/max)", FormatUtil.formatBytes(s.heapUsed())
                + " / " + FormatUtil.formatBytes(s.heapCommitted())
                + " / " + FormatUtil.formatBytes(s.heapMax())));
        resp.replyPrefixed(entry("Non-heap committed", FormatUtil.formatBytes(s.nonHeapCommitted())));
        for (Map.Entry<String, Long> pool : s.nonHeapPools().entrySet()) {
            resp.replyPrefixed(text("    " + pool.getKey() + ": " + FormatUtil.formatBytes(pool.getValue()), DARK_GRAY));
        }
        resp.replyPrefixed(entry("NIO direct buffers", FormatUtil.formatBytes(s.directUsed())
                + " in " + s.directCount() + " buffers"));
        if (s.mappedCount() > 0) {
            resp.replyPrefixed(entry("Mapped buffers", FormatUtil.formatBytes(s.mappedUsed())
                    + " in " + s.mappedCount() + " buffers"));
        }
        if (s.nettyDirect() >= 0) {
            String value = FormatUtil.formatBytes(s.nettyDirect());
            if (s.nettyMaxDirect() > 0) {
                value += " of a " + FormatUtil.formatBytes(s.nettyMaxDirect()) + " limit";
            }
            if (s.nettyArenas() > 0) {
                value += " across " + s.nettyArenas() + " pooled arenas";
            }
            resp.replyPrefixed(entry("Netty direct (pooled)", value));
        } else {
            // Reporting nothing here would read as zero, which is a different claim entirely.
            // Reflection into Netty fails under isolating plugin classloaders and no variant of it
            // will succeed there, so the honest answer is to say it is unmeasurable and name the
            // flags that measure it without reflection.
            resp.replyPrefixed(entry("Netty direct", "not measurable from this classloader"));
            resp.replyPrefixed(text("    use -XX:MaxDirectMemorySize=<n> to bound it and get an attributable", DARK_GRAY));
            resp.replyPrefixed(text("    OutOfMemoryError, or -Dio.netty.leakDetection.level=paranoid to catch", DARK_GRAY));
            resp.replyPrefixed(text("    buffers dropped without release() (misses buffers held by a strong ref).", DARK_GRAY));
        }
        resp.replyPrefixed(entry("Threads", s.threads() + " (peak " + s.peakThreads() + ")"
                + (s.threadStackSize() > 0 ? ", " + FormatUtil.formatBytes(s.threadStackSize()) + " stack each" : "")));
        resp.replyPrefixed(entry("Loaded classes", Long.toString(s.loadedClasses())));

        // LazyFree is the kernel's count of MADV_FREE pages: freed by the application, still
        // resident, and reclaimable the moment anything else needs them. A replacement allocator
        // that purges with MADV_FREE parks memory here indefinitely on a machine with headroom,
        // which is indistinguishable from a leak in every figure EXCEPT this one.
        Long lazyFree = s.smapsRollup().get("LazyFree");
        if (lazyFree != null && lazyFree > 0) {
            resp.replyPrefixed(entry("Freed but still resident (LazyFree)", FormatUtil.formatBytes(lazyFree)));
            if (lazyFree > 1024L * 1024 * 1024) {
                resp.replyPrefixed(text("    this memory is already released by the application and the kernel", DARK_GRAY));
                resp.replyPrefixed(text("    will reclaim it under pressure - it is NOT a leak. It is subtracted", DARK_GRAY));
                resp.replyPrefixed(text("    from the corrected figure below.", DARK_GRAY));
            }
        }

        String allocator = ProcessMemory.getPreloadedAllocator();
        if (allocator != null) {
            resp.replyPrefixed(entry("Allocator", allocator + " (preloaded, replaces glibc malloc)"));
        }

        long unaccounted = s.unaccounted();
        resp.replyPrefixed(text()
                .append(text("Unaccounted: ", GOLD))
                .append(text(formatSigned(unaccounted), unaccounted > 0 ? WHITE : GRAY))
                .build());
        resp.replyPrefixed(text("    resident size minus heap, non-heap, direct buffers and thread stacks", DARK_GRAY));
        // Apply the same corrections --investigate applies. Without this the three views report
        // three different figures for one process, and the text above tells the reader to subtract
        // LazyFree while the number beside it never does.
        appendUnaccountedCorrections(resp, s, unaccounted);
        if (unaccounted < 0) {
            resp.replyPrefixed(text("    negative because committed heap is not all resident - normal without", DARK_GRAY));
            resp.replyPrefixed(text("    AlwaysPreTouch, and it means there is no off-heap gap to explain.", DARK_GRAY));
        } else {
            resp.replyPrefixed(text("    this legitimately contains GC structures, glibc arenas and JNI memory -", DARK_GRAY));
            resp.replyPrefixed(text("    a large value is not a leak, a GROWING one is. Use --baseline then --diff.", DARK_GRAY));
        }
    }

    /**
     * Prints the corrections that make the raw unaccounted figure meaningful.
     *
     * <p>Shared so that the summary, the diagnosis and the investigation cannot drift apart. Three
     * commands quoting three different numbers for the same quantity is worse than any one of them
     * being slightly off, because it destroys the reader's ability to compare across runs.</p>
     */
    private long appendUnaccountedCorrections(CommandResponseHandler resp, ProcessMemorySnapshot s, long unaccounted) {
        if (unaccounted == Long.MIN_VALUE || unaccounted <= 0) {
            return unaccounted;
        }
        long corrected = unaccounted;
        List<String> notes = new ArrayList<>();

        long nmtThreads = DiagnosticCommand.getNmtCommitted("Thread");
        if (nmtThreads >= 0) {
            long stackSize = s.threadStackSize() > 0 ? s.threadStackSize() : 1024L * 1024L;
            long overcount = ((long) s.threads() * stackSize) - nmtThreads;
            if (overcount > 64L * 1024 * 1024) {
                corrected += overcount;
                notes.add("thread stacks over-counted by " + FormatUtil.formatBytes(overcount));
            }
        }

        Long fileBacked = s.smapsRollup().get("Pss_File");
        if (fileBacked != null && fileBacked > 64L * 1024 * 1024) {
            corrected -= fileBacked;
            notes.add("file-backed resident (jars, .so, mapped files): " + FormatUtil.formatBytes(fileBacked));
        }

        Long lazyFree = s.smapsRollup().get("LazyFree");
        if (lazyFree != null && lazyFree > 64L * 1024 * 1024) {
            corrected -= lazyFree;
            notes.add("LazyFree (already released, reclaimable): " + FormatUtil.formatBytes(lazyFree));
        }

        if (notes.isEmpty()) {
            return unaccounted;
        }
        for (String note : notes) {
            resp.replyPrefixed(text("    - " + note, DARK_GRAY));
        }
        resp.replyPrefixed(text("    corrected unaccounted: " + formatSigned(corrected), GRAY));
        return corrected;
    }

    private void reportDiff(CommandResponseHandler resp, ProcessMemorySnapshot from, ProcessMemorySnapshot to) {
        long millis = Math.max(1, to.timestamp() - from.timestamp());
        long minutes = Math.max(1, millis / 60_000);

        resp.replyPrefixed(text("Change over " + minutes + " minutes", GOLD));
        resp.replyPrefixed(delta("Resident set size", sentinel(from.rss()), sentinel(to.rss()), millis));
        resp.replyPrefixed(delta("Heap committed", from.heapCommitted(), to.heapCommitted(), millis));
        resp.replyPrefixed(delta("Non-heap", from.nonHeapCommitted(), to.nonHeapCommitted(), millis));
        resp.replyPrefixed(delta("NIO direct", from.directUsed(), to.directUsed(), millis));
        if (from.nettyDirect() >= 0 && to.nettyDirect() >= 0) {
            resp.replyPrefixed(delta("Netty direct", from.nettyDirect(), to.nettyDirect(), millis));
        }

        resp.replyPrefixed(entry("Threads", from.threads() + " -> " + to.threads()));
        resp.replyPrefixed(delta("Unaccounted", from.unaccounted(), to.unaccounted(), millis));

        if (minutes < 30) {
            resp.replyPrefixed(text("Short window - extrapolated rates from under 30 minutes are noisy.", DARK_GRAY));
        }
    }

    private void reportMappings(CommandResponseHandler resp, int requestedTop) {
        List<ProcessMemory.Mapping> mappings = ProcessMemory.getMappings();
        if (mappings.isEmpty()) {
            resp.replyPrefixed(text("Could not read /proc/self/smaps.", RED));
            return;
        }

        int top = requestedTop == -1 ? 12 : Math.min(60, Math.max(1, requestedTop));
        long totalRss = mappings.stream().mapToLong(ProcessMemory.Mapping::rss).sum();
        long anonRss = mappings.stream().filter(ProcessMemory.Mapping::anonymous)
                .mapToLong(ProcessMemory.Mapping::rss).sum();
        int arenaLike = ProcessMemory.countArenaLikeRegions(mappings);
        long stackLike = mappings.stream()
                .filter(m -> m.anonymous() && m.size() >= 512 * 1024 && m.size() <= 8L * 1024 * 1024)
                .count();

        resp.replyPrefixed(text("Memory mappings", GOLD));
        resp.replyPrefixed(entry("Total", mappings.size() + " mappings, "
                + FormatUtil.formatBytes(totalRss) + " resident (" + FormatUtil.formatBytes(anonRss) + " anonymous)"));
        resp.replyPrefixed(entry("glibc arena-shaped regions (64 MiB)", Integer.toString(arenaLike)));
        long mainArena = ProcessMemory.getMainArenaResident(mappings);
        if (mainArena > 0) {
            resp.replyPrefixed(entry("glibc main arena [heap]", FormatUtil.formatBytes(mainArena)));
            if (mainArena > 1024L * 1024 * 1024) {
                resp.replyPrefixed(text("    MALLOC_ARENA_MAX does NOT cap this one - it limits how many", DARK_GRAY));
                resp.replyPrefixed(text("    secondary arenas exist, not the size of the main arena.", DARK_GRAY));
            }
        }
        resp.replyPrefixed(entry("Small anonymous regions (thread stacks)", Long.toString(stackLike)));

        // Judge arena retention by BYTES, not by region count. Sixteen arenas is 1 GiB; saying
        // "this is your problem" next to 25 GiB of other anonymous memory points the reader at
        // the wrong thing entirely. Only claim it when it is actually a meaningful share.
        // Sum the ACTUAL resident bytes of the arena-shaped mappings. An earlier version multiplied
        // the region count by 64 MiB, which is the reservation size, not what is resident - so a
        // set of barely-touched subheaps was reported as holding their full nominal size. That is
        // the same count-based estimate this warning was supposedly changed away from, just
        // wearing a bytes label, and it produced figures larger than the total anonymous memory
        // they were being compared against.
        long arenaBytes = mappings.stream()
                .filter(m -> m.anonymous() && m.size() >= 60L * 1024 * 1024 && m.size() <= 68L * 1024 * 1024)
                .mapToLong(ProcessMemory.Mapping::rss)
                .sum();
        String preloaded = ProcessMemory.getPreloadedAllocator();
        if (preloaded != null) {
            resp.replyPrefixed(text("Allocator: " + preloaded + " is preloaded in place of glibc malloc.", YELLOW));
            resp.replyPrefixed(text("    64 MiB regions here are its arenas, not glibc's - MALLOC_ARENA_MAX", DARK_GRAY));
            resp.replyPrefixed(text("    does not apply, and neither does --trim (that calls glibc malloc_trim).", DARK_GRAY));
        } else if (arenaLike >= 8 && (arenaBytes > 2L * 1024 * 1024 * 1024 || arenaBytes * 10 > anonRss)) {
            resp.replyPrefixed(text("Arena-shaped regions hold ~" + FormatUtil.formatBytes(arenaBytes)
                    + " - a significant share here. Likely glibc arena retention, not a leak.", YELLOW));
            resp.replyPrefixed(text("Set MALLOC_ARENA_MAX=2 in the environment; confirm with --trim first.", YELLOW));
        } else if (arenaLike > 0) {
            resp.replyPrefixed(text("    (arena-shaped regions account for ~" + FormatUtil.formatBytes(arenaBytes)
                    + " - too small to explain the total)", DARK_GRAY));
        }

        List<ProcessMemory.Mapping> sorted = mappings.stream()
                .sorted(Comparator.comparingLong(ProcessMemory.Mapping::rss).reversed())
                .collect(java.util.stream.Collectors.toList());

        resp.replyPrefixed(text("Largest by resident size:", GOLD));
        sorted.stream().limit(top).forEach(m -> resp.replyPrefixed(text("    "
                + FormatUtil.formatBytes(m.rss()) + "  " + m.permissions() + "  "
                + (m.anonymous() ? "[anonymous]" : m.path()), GRAY)));

        // The tail is the point when no single mapping dominates. Printing the top N without
        // saying what is left below them invites the reader to conclude the list explains the
        // total, when thousands of small mappings may hold far more than the visible few.
        long shown = sorted.stream().limit(top).mapToLong(ProcessMemory.Mapping::rss).sum();
        long tail = totalRss - shown;
        if (tail > 0) {
            resp.replyPrefixed(text("    ... " + (sorted.size() - Math.min(top, sorted.size()))
                    + " smaller mappings holding " + FormatUtil.formatBytes(tail) + " between them", GRAY));
        }

        // Native libraries loaded into the process. When NMT accounts for far less than the
        // process is resident, the remainder was allocated by native code outside the JVM's
        // tracking - and the only way to get from "25 GiB is unaccounted" to a name is to know
        // which native code is even present. A JNI library's own malloc/mmap never appears in
        // NMT, so this list is usually where the answer starts.
        java.util.Map<String, long[]> libs = new java.util.TreeMap<>();
        for (ProcessMemory.Mapping m : sorted) {
            String path = m.path();
            // "(deleted)" is appended by the kernel for unlinked files, and temp-extracted natives
            // are common - spark extracts its own profiler library that way. Matching only on a
            // trailing .so misses both.
            if (path.contains(".so")) {
                long[] agg = libs.computeIfAbsent(path, k -> new long[2]);
                agg[0] += m.rss();
                agg[1]++;
            }
        }
        if (!libs.isEmpty()) {
            resp.replyPrefixed(text("Native libraries loaded (" + libs.size() + "):", GOLD));
            libs.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue()[0], a.getValue()[0]))
                    .limit(15)
                    .forEach(e -> {
                        String name = e.getKey().substring(e.getKey().lastIndexOf('/') + 1);
                        resp.replyPrefixed(text("    " + FormatUtil.formatBytes(e.getValue()[0])
                                + "  " + name, GRAY));
                    });
            resp.replyPrefixed(text("    (their own heap allocations are NOT counted here - this is", DARK_GRAY));
            resp.replyPrefixed(text("     only their mapped code and data)", DARK_GRAY));
        }

        // Heap vs everything else is the split that actually matters. Under ZGC the heap is a
        // memfd mapping rather than anonymous memory, so a reader who assumes "anonymous = heap"
        // draws exactly the wrong conclusion about where the memory went.
        long heapMapped = sorted.stream()
                .filter(m -> m.path().contains("java_heap") || m.path().contains("memfd:java"))
                .mapToLong(ProcessMemory.Mapping::rss).sum();
        if (heapMapped > 0) {
            resp.replyPrefixed(text("Java heap is file-backed here (ZGC memfd): "
                    + FormatUtil.formatBytes(heapMapped), GOLD));
            resp.replyPrefixed(text("    non-heap resident therefore ~"
                    + FormatUtil.formatBytes(totalRss - heapMapped) + " - that is the figure to explain", DARK_GRAY));
        }
    }

    private void reportNativeMemoryTracking(SparkPlatform platform, CommandResponseHandler resp, Arguments arguments) {
        if (!DiagnosticCommand.isNativeMemoryTrackingEnabled()) {
            resp.replyPrefixed(text("Native Memory Tracking is not enabled.", RED));
            resp.replyPrefixed(text("Add -XX:NativeMemoryTracking=summary to the startup flags and restart.", GRAY));
            resp.replyPrefixed(text("It cannot be enabled at runtime. NMT is the only source that says which", GRAY));
            resp.replyPrefixed(text("JVM region grew; everything else in this command works without it.", GRAY));
            return;
        }

        String output;
        if (arguments.boolFlag("nmt-baseline")) {
            output = DiagnosticCommand.execute("VM.native_memory", "baseline");
            resp.replyPrefixed(text("NMT baseline recorded. Run --nmt-diff later.", GOLD));
            resp.replyPrefixed(text(output.trim(), GRAY));
            return;
        } else if (arguments.boolFlag("nmt-diff")) {
            output = DiagnosticCommand.execute("VM.native_memory", "summary.diff", "scale=MB");
        } else {
            output = DiagnosticCommand.execute("VM.native_memory", "summary", "scale=MB");
        }

        for (String line : output.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("-") || trimmed.startsWith("Total:")) {
                resp.replyPrefixed(text(trimmed, GRAY));
            }
        }
        writeToPluginDirectory(platform, resp, "nmt", output);
    }

    private void trimNativeHeap(CommandResponseHandler resp) {
        String preloaded = ProcessMemory.getPreloadedAllocator();
        if (preloaded != null) {
            // A zero result would otherwise read as "the memory is genuinely in use", which is a
            // conclusion this command has no basis for when the allocator being trimmed is not
            // the one holding the memory.
            resp.replyPrefixed(text(preloaded + " is preloaded in place of glibc malloc.", YELLOW));
            resp.replyPrefixed(text("System.trim_native_heap calls glibc malloc_trim, which does not control", GRAY));
            resp.replyPrefixed(text("this allocator's retention - a zero result here would mean nothing.", GRAY));
            resp.replyPrefixed(text("Check the LazyFree figure in the summary instead.", GRAY));
            return;
        }

        long before = ProcessMemory.getResidentSetSize();
        String output = DiagnosticCommand.execute("System.trim_native_heap");

        if (output.startsWith(DiagnosticCommand.UNAVAILABLE)) {
            resp.replyPrefixed(text("System.trim_native_heap is not available on this JVM.", RED));
            resp.replyPrefixed(text("It requires a glibc Linux JVM. Use --maps to inspect arena shape instead.", GRAY));
            return;
        }

        long after = ProcessMemory.getResidentSetSize();
        long delta = before - after;

        resp.replyPrefixed(entry("Resident before", FormatUtil.formatBytes(before)));
        resp.replyPrefixed(entry("Resident after", FormatUtil.formatBytes(after)));
        resp.replyPrefixed(entry("Change", formatSigned(-delta)));

        if (delta < 0) {
            // Clamping this to zero previously turned a measurement artifact into a finding: on a
            // busy server ordinary allocation during the trim can outweigh what was returned, and
            // the command would announce "the memory is genuinely in use" on that basis alone.
            resp.replyPrefixed(text("Resident size GREW during the trim - the process allocated faster than", GRAY));
            resp.replyPrefixed(text("the trim released. This says nothing either way; retry when quieter.", GRAY));
            return;
        }

        // Judged against what glibc actually holds, not a fixed byte count. 512 MB is decisive on
        // a 5 GB process and noise on a 90 GB one.
        GlibcArenaInfo arenas = GlibcArenaInfo.capture();
        long held = arenas.isAvailable() ? arenas.totalHeldBytes() : 0;
        boolean large = held > 0 ? delta * 4 > held : delta > 512L * 1024 * 1024;

        if (large) {
            resp.replyPrefixed(text("A large share of what glibc held was returned - it was holding freed", YELLOW));
            resp.replyPrefixed(text("memory, not leaking it. Set MALLOC_ARENA_MAX=2 to stop it accumulating.", YELLOW));
        } else if (held > 0) {
            resp.replyPrefixed(text(String.format("Only %s of the %s glibc holds was returned - the rest is live.",
                    FormatUtil.formatBytes(delta), FormatUtil.formatBytes(held)), GRAY));
        } else {
            resp.replyPrefixed(text("Little was returned - the memory appears to be genuinely in use.", GRAY));
        }
    }

    private void reportFlags(CommandResponseHandler resp) {
        resp.replyPrefixed(text("Memory-relevant JVM configuration", GOLD));
        for (String argument : java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            String lower = argument.toLowerCase();
            if (lower.contains("mx") || lower.contains("ms") || lower.contains("direct")
                    || lower.contains("nativememory") || lower.contains("metaspace")
                    || lower.contains("codecache") || lower.contains("stack")
                    // -Xss is the actual spelling; "stack" alone never matches it, so the one
                    // setting the thread-stack corrections depend on was never displayed.
                    || lower.startsWith("-xss") || lower.startsWith("-xmn")
                    || lower.contains("pretouch") || lower.contains("gc")) {
                resp.replyPrefixed(text("    " + argument, GRAY));
            }
        }

        String maxDirect = DiagnosticCommand.getVmOption("MaxDirectMemorySize");
        boolean unsetDirect = maxDirect == null || maxDirect.equals("0");
        resp.replyPrefixed(entry("MaxDirectMemorySize", unsetDirect
                ? "unset - direct buffers may grow as large as the heap" : maxDirect));
        if (unsetDirect) {
            resp.replyPrefixed(text("    setting this converts a slow OOM-kill into an attributable stack trace", DARK_GRAY));
        }

        String arenaMax = System.getenv("MALLOC_ARENA_MAX");
        resp.replyPrefixed(entry("MALLOC_ARENA_MAX", arenaMax == null ? "unset" : arenaMax));
        resp.replyPrefixed(entry("Available processors", Integer.toString(Runtime.getRuntime().availableProcessors())
                + " (glibc permits up to 8x this many 64 MiB arenas)"));
    }

    /**
     * Uploads a memory accounting snapshot to bytebin and returns a viewer link.
     *
     * <p>Deliberately posted as {@code application/x-spark-sampler} - the same content type and
     * the same {@code SamplerData} message as any other spark profile - rather than inventing a
     * new one. Two reasons. Proto3 preserves unknown fields, so the stock viewer accepts it and
     * simply shows an empty profile instead of an error; and any tool that already parses spark
     * profiles gets this for free rather than needing a second code path.</p>
     *
     * <p>{@code memory_accounting_only} is set so a reader can tell this apart from a profile
     * whose sampled trees happen to be empty. Reporting "no CPU hotspots found" for an upload
     * that never sampled CPU would be a false negative dressed up as a result.</p>
     */
    private void upload(SparkPlatform platform, CommandSender sender, CommandResponseHandler resp, Arguments arguments) {
        boolean includeMappings = arguments.boolFlag("maps");
        resp.replyPrefixed(text("Collecting memory accounting"
                + (includeMappings ? " including mappings (this reads smaps and may take a moment)" : "")
                + "...", GRAY));

        ProcessMemorySnapshot snapshot = ProcessMemorySnapshot.capture(includeMappings, true);

        SamplerMetadata.Builder metadata = SamplerMetadata.newBuilder();
        SparkMetadata.gather(platform, sender.toData(), platform.getStartupGcStatistics()).writeTo(metadata);

        SamplerData data = SamplerData.newBuilder()
                .setMetadata(metadata)
                .setProcessMemory(snapshot.toProto())
                .setExtendedContents(SparkSamplerProtos.ExtendedProfileContents.newBuilder()
                        .setHasExecution(false)
                        .setHasProcessMemory(true)
                        .setMemoryAccountingOnly(true)
                        .setForkVersion(AsyncSampler.forkVersion(platform))
                        .build())
                .build();

        try {
            String key = platform.getBytebinClient().postContent(data, MediaTypes.SPARK_SAMPLER_MEDIA_TYPE).key();
            String url = platform.getViewerUrl() + key;

            resp.broadcastPrefixed(text("Memory accounting snapshot:", GOLD));
            resp.broadcast(text()
                    .content(url)
                    .color(GRAY)
                    .clickEvent(ClickEvent.openUrl(url))
                    .build());

            platform.getActivityLog().addToLog(Activity.urlActivity(
                    resp.senderData(), System.currentTimeMillis(), "Memory accounting", url));
        } catch (Throwable t) {
            resp.replyPrefixed(text("An error occurred whilst uploading the data.", RED));
            platform.getPlugin().log(Level.SEVERE, "Error uploading memory accounting", t);
        }
    }

    /**
     * Reads an int flag, falling back to {@code def} when the flag was given without a value.
     *
     * <p>A flag with no value parses to an empty string rather than to nothing, so
     * {@link Arguments#intFlag} never reaches its -1 "undefined" return: it calls
     * {@code Integer.parseInt("")} and throws "Please specify a number!" instead. Both call sites
     * offer a bare form as the default - and both were rejecting it.</p>
     */
    private static int intFlagOrDefault(Arguments arguments, String key, int def) {
        return flagValue(arguments, key) == null ? def : arguments.intFlag(key);
    }

    /**
     * The value given for a flag, or null if the flag was given bare.
     */
    private static String flagValue(Arguments arguments, String key) {
        Iterator<String> it = arguments.stringFlag(key).iterator();
        if (!it.hasNext()) {
            return null;
        }
        String value = it.next().trim();
        return value.isEmpty() ? null : value;
    }

    /**
     * Reads or sets Netty's leak detection level.
     *
     * <p>Exists so the level can be raised without {@code -Dio.netty.leakDetection.level} and a
     * restart. On a server losing direct memory to its network stack the restart is the problem:
     * it clears the leak along with the evidence, and the operator has to wait for it to build up
     * again before they can look.</p>
     */
    private void configureNettyLeakDetection(CommandResponseHandler resp, Arguments arguments) {
        String current = NettyLeakDetector.currentLevel();
        if (current == null) {
            resp.replyPrefixed(text("Netty's leak detector is not reachable on this server.", RED));
            resp.replyPrefixed(text("io.netty.util.ResourceLeakDetector could not be loaded - either", GRAY));
            resp.replyPrefixed(text("netty is absent, or this platform relocates it into another package.", GRAY));
            return;
        }

        String requested = flagValue(arguments, "netty-leak");
        if (requested == null) {
            resp.replyPrefixed(text("Netty leak detection: ", GOLD).append(text(current, WHITE)));
            resp.replyPrefixed(text("Levels: " + String.join(", ", NettyLeakDetector.levels()), GRAY));
            return;
        }

        if (!NettyLeakDetector.setLevel(requested)) {
            resp.replyPrefixed(text("Not a level: " + requested, RED));
            resp.replyPrefixed(text("Use one of: " + String.join(", ", NettyLeakDetector.levels()), GRAY));
            return;
        }

        String now = NettyLeakDetector.currentLevel();
        resp.replyPrefixed(text("Netty leak detection: ", GREEN)
                .append(text(current, GRAY))
                .append(text(" -> ", DARK_GRAY))
                .append(text(now == null ? requested : now, WHITE)));

        // Everything below is what makes the setting usable rather than merely applied.
        resp.replyPrefixed(text("Reports go to the SERVER log, not to spark - netty writes them", GRAY));
        resp.replyPrefixed(text("through its own logger. Search the log for 'LEAK:'.", GRAY));
        resp.replyPrefixed(text("A leak is only reported once the buffer is garbage collected, so", GRAY));
        resp.replyPrefixed(text("expect nothing until the next few GCs have run.", GRAY));
        resp.replyPrefixed(text("This finds unreleased netty ByteBufs only. For malloc, mapped files", GRAY));
        resp.replyPrefixed(text("or arena retention use --investigate.", GRAY));

        String lower = requested.toLowerCase(Locale.ROOT);
        if (lower.equals("paranoid")) {
            resp.replyPrefixed(text("PARANOID tracks every buffer and is very expensive - it is a", YELLOW));
            resp.replyPrefixed(text("short-window setting, not one to leave on.", YELLOW));
        } else if (lower.equals("advanced")) {
            resp.replyPrefixed(text("ADVANCED samples about 1% of buffers and records access traces.", YELLOW));
            resp.replyPrefixed(text("Measurable overhead; fine for hours, not for good.", YELLOW));
        }
        resp.replyPrefixed(text("The level resets to the JVM default on restart.", DARK_GRAY));
    }

    // ------------------------------------------------------------------- watch & dump

    private void configureWatch(SparkPlatform platform, CommandResponseHandler resp, int minutes) {
        cancelWatch();

        if (minutes <= 0) {
            resp.replyPrefixed(text("Memory watch stopped.", GOLD));
            return;
        }

        Path file = platform.getPlugin().getPluginDirectory().resolve("memory-history.csv");
        this.watchTask = MonitoringExecutor.INSTANCE.scheduleAtFixedRate(
                () -> appendHistory(platform, file), 0, minutes, TimeUnit.MINUTES);

        resp.replyPrefixed(text("Recording a row every " + minutes + " minutes to:", GOLD));
        resp.replyPrefixed(text("    " + file, GRAY));
        resp.replyPrefixed(text("Download it from the file manager and plot 'unaccounted' over time.", GRAY));
        resp.replyPrefixed(text("A straight climb is a leak; a curve that flattens is a cache filling up.", GRAY));
    }

    private void cancelWatch() {
        if (this.watchTask != null) {
            this.watchTask.cancel(false);
            this.watchTask = null;
        }
    }

    /**
     * Appends one row.
     *
     * <p>Catches {@link Throwable}, not {@link IOException}. A task submitted to
     * scheduleAtFixedRate is cancelled permanently the first time it throws, so an unchecked
     * exception here would stop the recording for the rest of the server's uptime while
     * appearing to still be running - and the whole value of this feature is the long
     * uninterrupted series. One bad sample is worth losing; the series is not.</p>
     */
    private void appendHistory(SparkPlatform platform, Path file) {
        try {
            ProcessMemorySnapshot s = ProcessMemorySnapshot.capture();
            Files.createDirectories(file.getParent());

            if (Files.exists(file) && Files.size(file) > MAX_HISTORY_BYTES) {
                platform.getPlugin().log(Level.WARNING, "Memory history file has reached "
                        + FormatUtil.formatBytes(MAX_HISTORY_BYTES) + "; recording stopped. "
                        + "Move or delete " + file + " to resume.");
                cancelWatch();
                return;
            }

            StringBuilder sb = new StringBuilder();
            if (!Files.exists(file)) {
                sb.append("time,rss,heap_used,heap_committed,non_heap,nio_direct,netty_direct,")
                        .append("threads,classes,unaccounted,cgroup_current\n");
            }
            Long cgroupCurrent = s.cgroup().get("memory.current");
            sb.append(ISO.format(Instant.ofEpochMilli(s.wallTimestamp()))).append(',')
                    .append(s.rss()).append(',')
                    .append(s.heapUsed()).append(',')
                    .append(s.heapCommitted()).append(',')
                    .append(s.nonHeapCommitted()).append(',')
                    .append(s.directUsed()).append(',')
                    .append(s.nettyDirect()).append(',')
                    .append(s.threads()).append(',')
                    .append(s.loadedClasses()).append(',')
                    .append(s.unaccounted() == Long.MIN_VALUE ? "" : String.valueOf(s.unaccounted())).append(',')
                    .append(cgroupCurrent == null ? -1 : cgroupCurrent).append('\n');

            Files.write(file, sb.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Throwable t) {
            platform.getPlugin().log(Level.WARNING, "Unable to append memory history", t);
        }
    }

    /**
     * Walks the whole off-heap decision tree in one command and states a verdict.
     *
     * <p>Every measurement here is available separately, but the diagnosis lives in the
     * RELATIONSHIPS between them, and reconstructing those by hand is where the time goes.
     * Resident size alone says nothing. Resident size minus what the JVM accounts for, checked
     * against what the allocator admits to holding and how much of that is already free, is a
     * diagnosis - and it separates the two cases that look identical from every other angle:
     * allocator fragmentation, which a tunable fixes, and a genuine leak, which needs a caller
     * found.</p>
     */
    private void diagnose(CommandResponseHandler resp) {
        ProcessMemorySnapshot s = ProcessMemorySnapshot.capture();
        GlibcArenaInfo arenas = GlibcArenaInfo.capture();
        String allocator = ProcessMemory.getPreloadedAllocator();
        long unaccounted = s.unaccounted();

        resp.replyPrefixed(text("Off-heap diagnosis", GOLD));
        resp.replyPrefixed(entry("Resident set size", formatSigned(s.rss())));
        long stacks = (long) s.threads() * (s.threadStackSize() > 0 ? s.threadStackSize() : 1024L * 1024L);
        resp.replyPrefixed(entry("Accounted by the JVM", FormatUtil.formatBytes(
                s.heapCommitted() + s.nonHeapCommitted() + s.directUsed() + stacks)
                + " (incl. " + FormatUtil.formatBytes(stacks) + " thread stacks)"));
        resp.replyPrefixed(entry("Unaccounted", formatSigned(unaccounted)));
        // The verdict must reason about the SAME figure printed to the reader. Previously the
        // corrections were displayed and then ignored, so the conclusion could contradict the
        // number directly above it.
        long correctedUnaccounted = appendUnaccountedCorrections(resp, s, unaccounted);

        Long lazyFree = s.smapsRollup().get("LazyFree");
        if (lazyFree != null && lazyFree > 0) {
            resp.replyPrefixed(entry("Freed but resident (LazyFree)", FormatUtil.formatBytes(lazyFree)));
        }

        if (allocator != null) {
            resp.replyPrefixed(entry("Allocator", allocator + " (preloaded, replaces glibc)"));
            resp.replyPrefixed(text("    glibc arena statistics and MALLOC_ARENA_MAX do not apply here.", DARK_GRAY));
            return;
        }
        if (!arenas.isAvailable()) {
            resp.replyPrefixed(text("glibc arena statistics unavailable on this JVM or platform.", GRAY));
            return;
        }

        resp.replyPrefixed(text("glibc allocator", GOLD));
        resp.replyPrefixed(entry("Arenas", Integer.toString(arenas.arenas())));
        resp.replyPrefixed(entry("Subheaps (64 MiB each)", Integer.toString(arenas.subheaps())));
        resp.replyPrefixed(entry("Held from the OS", FormatUtil.formatBytes(arenas.totalHeldBytes())
                + (arenas.mmapBytes() > 0
                        ? " (" + FormatUtil.formatBytes(arenas.systemBytes()) + " in arenas, "
                          + FormatUtil.formatBytes(arenas.mmapBytes()) + " mmap-served in "
                          + arenas.mmapCount() + " blocks)"
                        : "")));
        resp.replyPrefixed(entry("Of which free", FormatUtil.formatBytes(arenas.freeBytes())
                + String.format(" (%.0f%% of all held", arenas.freeRatio() * 100)
                + (arenas.mmapBytes() > 0
                        ? String.format(", %.0f%% of arena memory)", arenas.arenaFreeRatio() * 100)
                        : ")")));

        String arenaMax = System.getenv("MALLOC_ARENA_MAX");
        int cores = Runtime.getRuntime().availableProcessors();
        resp.replyPrefixed(entry("MALLOC_ARENA_MAX", arenaMax == null
                ? "unset - up to " + (cores * 8) + " arenas on " + cores + " cores" : arenaMax));

        resp.replyPrefixed(text("Verdict", GOLD));
        long held = arenas.totalHeldBytes();
        if (correctedUnaccounted <= 0 || held * 2 <= correctedUnaccounted) {
            resp.replyPrefixed(text("    glibc holds " + FormatUtil.formatBytes(held) + " of "
                    + formatSigned(correctedUnaccounted) + " unaccounted - not the main consumer.", GRAY));
            resp.replyPrefixed(text("    Look elsewhere: --nmt-diff for JVM regions, --maps for mapping", GRAY));
            resp.replyPrefixed(text("    shape and loaded native libraries.", GRAY));
        } else if (arenas.freeRatio() > 0.4) {
            resp.replyPrefixed(text("    glibc holds most of the unaccounted memory and "
                    + String.format("%.0f%%", arenas.freeRatio() * 100) + " of it is FREE.", YELLOW));
            resp.replyPrefixed(text("    That is arena fragmentation, not a leak - the allocations were", YELLOW));
            if (arenaMax == null) {
                resp.replyPrefixed(text("    released; glibc never returned the pages. Set MALLOC_ARENA_MAX=2", YELLOW));
                resp.replyPrefixed(text("    (or 8 if malloc contention costs you tick time), and --trim now.", YELLOW));
            } else {
                // Recommending a setting that is already applied wastes the reader's time and
                // undermines every other line of the verdict.
                resp.replyPrefixed(text("    released; glibc never returned the pages. The arena cap is already", YELLOW));
                resp.replyPrefixed(text("    at " + arenaMax + " - run --trim to reclaim the free portion now.", YELLOW));
            }
        } else {
            resp.replyPrefixed(text("    glibc holds most of the unaccounted memory but only "
                    + String.format("%.0f%%", arenas.freeRatio() * 100) + " is free.", YELLOW));
            resp.replyPrefixed(text("    The arenas hold LIVE allocations, so something is genuinely not", YELLOW));
            resp.replyPrefixed(text("    freeing. Capture: /spark profiler start --leaks --thread *", YELLOW));
        }
        resp.replyPrefixed(text("    Growth separates a leak from a large steady state - run --baseline", DARK_GRAY));
        resp.replyPrefixed(text("    then --diff, or --watch 15, before changing anything.", DARK_GRAY));
    }

    /**
     * Runs an arbitrary jcmd diagnostic command, refusing the destructive ones.
     *
     * <p>An unrestricted passthrough is a loaded gun on a production server: GC.heap_dump writes
     * tens of gigabytes and stalls the JVM, VM.set_flag mutates the running VM. Read-only
     * commands stay available; the refusal names the reason so it is obvious how to run one
     * deliberately from a shell if that is genuinely wanted.</p>
     */
    private void runDiagnosticCommand(SparkPlatform platform, CommandResponseHandler resp, Arguments arguments) {
        Set<String> values = arguments.stringFlag("jcmd");
        if (values.isEmpty()) {
            resp.replyPrefixed(text("Usage: --jcmd <command>, e.g. --jcmd VM.flags", RED));
            resp.replyPrefixed(text("Try: VM.flags, VM.metaspace, System.native_heap_info, GC.heap_info", GRAY));
            return;
        }

        String command = values.iterator().next();
        // Locale.ROOT: the blocklist below is matched on a lower-cased copy of a command NAME.
        // Under a Turkish locale the default folds 'I' to a dotless one, so a typed
        // "COMPILER.DIRECTIVES" would not match "compiler.directives" and the guard that refuses
        // destructive commands would be bypassed by nothing more than the server's locale.
        String lower = command.toLowerCase(Locale.ROOT);
        for (String blocked : DESTRUCTIVE_COMMANDS) {
            if (lower.startsWith(blocked.toLowerCase(Locale.ROOT))) {
                resp.replyPrefixed(text(command + " is not available through this command.", RED));
                resp.replyPrefixed(text("It writes huge files, mutates the VM, or pauses it for a long time.", GRAY));
                resp.replyPrefixed(text("Run it from a shell with jcmd if you have decided you want it.", GRAY));
                return;
            }
        }
        for (String expensive : EXPENSIVE_COMMANDS) {
            if (lower.startsWith(expensive.toLowerCase(Locale.ROOT))) {
                resp.replyPrefixed(text("Note: " + command + " walks a large structure or brings every", YELLOW));
                resp.replyPrefixed(text("thread to a safepoint, and can pause a big server noticeably.", YELLOW));
                break;
            }
        }

        String output = DiagnosticCommand.execute(command);
        if (output.startsWith(DiagnosticCommand.UNAVAILABLE)) {
            resp.replyPrefixed(text(command + " is not available: ", RED).append(text(output, GRAY)));
            return;
        }

        // String.lines() is Java 11; spark-common targets Java 8.
        String[] lines = output.split("\n");
        for (int i = 0; i < Math.min(25, lines.length); i++) {
            resp.replyPrefixed(text(lines[i], GRAY));
        }
        if (lines.length > 25) {
            resp.replyPrefixed(text("    ... " + (lines.length - 25) + " more lines", DARK_GRAY));
        }
        writeToPluginDirectory(platform, resp, "jcmd-" + command.replace('.', '_'), output);
    }

    /**
     * Runs a timed investigation and writes a correlated report.
     *
     * <p>Deliberately long by default. Nearly every wrong conclusion this command exists to
     * prevent came from a short window: a startup ramp reads as a leak, a GC cycle reads as a
     * plateau, and a bursty allocator looks idle between bursts.</p>
     */
    private void startInvestigation(SparkPlatform platform, CommandResponseHandler resp, int minutes) {
        // 0 or negative cancels. Without this a mistyped duration commits the operator to
        // waiting it out or reloading the plugin.
        if (minutes <= 0) {
            if (this.investigation == null) {
                resp.replyPrefixed(text("No investigation is running.", GRAY));
            } else {
                cancelInvestigation();
                resp.replyPrefixed(text("Investigation cancelled.", GOLD));
            }
            return;
        }

        OffHeapInvestigation running = this.investigation;
        if (running != null) {
            resp.replyPrefixed(text("An investigation is already running.", GOLD));
            resp.replyPrefixed(text("  " + running.progress(), GRAY));
            resp.replyPrefixed(text("Use --investigate 0 to cancel it.", GRAY));
            return;
        }

        // Clamp: under 5 minutes produces rates dominated by noise, and an unbounded value would
        // schedule a report days out while holding a thread and a set of smaps snapshots.
        if (minutes < 5) {
            minutes = 5;
            resp.replyPrefixed(text("Raised to the 5 minute minimum - shorter windows measure noise.", GRAY));
        } else if (minutes > 720) {
            minutes = 720;
            resp.replyPrefixed(text("Capped at 12 hours.", GRAY));
        }

        OffHeapInvestigation inv = new OffHeapInvestigation();
        this.investigation = inv;

        long intervalMinutes = Math.max(1, minutes / 12);
        resp.replyPrefixed(text("Investigating off-heap memory for " + minutes + " minutes.", GOLD));
        resp.replyPrefixed(text("Sampling every " + intervalMinutes + "m. Leave the server under normal load.", GRAY));
        resp.replyPrefixed(text("The report is written to the spark plugin folder when it finishes.", GRAY));

        ScheduledExecutorService executor = new SparkScheduledThreadPoolExecutor(1,
                new SparkThreadFactory("spark-offheap-investigation", true));
        this.investigationExecutor = executor;

        // Schedule everything BEFORE the opening sample runs. If begin() fails it cancels the
        // investigation, which shuts this executor down - and the two schedule calls below would
        // then be rejected, surfacing as an exception on the command thread rather than the clean
        // failure message the catch is trying to produce.
        this.investigationTask = executor.scheduleAtFixedRate(() -> {
            try {
                inv.sample();
            } catch (Throwable t) {
                platform.getPlugin().log(Level.WARNING, "Investigation sample failed", t);
            }
        }, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);

        executor.execute(() -> {
            try {
                inv.begin(platform.getPlugin().getPluginDirectory()
                        .resolve("investigation-live-" + FILE_STAMP.format(Instant.now()) + ".csv"));
            } catch (Throwable t) {
                platform.getPlugin().log(Level.WARNING, "Investigation failed to start", t);
                cancelInvestigation();
                resp.replyPrefixed(text("Investigation failed to start - see the server log.", RED));
            }
        });

        executor.schedule(() -> {
            try {
                if (this.investigationTask != null) {
                    this.investigationTask.cancel(false);
                    this.investigationTask = null;
                }
                inv.sample();
                // Build the report ONCE. It runs an NMT summary.diff internally, so calling it
                // twice both wastes the work and risks the file and the chat output disagreeing.
                List<String> reportLines = inv.report();
                StringBuilder sb = new StringBuilder();
                for (String line : reportLines) {
                    sb.append(line).append('\n');
                }
                this.investigation = null;

                // Write the file BEFORE attempting to reply. The command sender may have
                // disconnected during a two-hour run, and losing the entire report because a
                // chat message could not be delivered would be the worst possible failure here.
                Path file = writeReportFile(platform, sb.toString());
                platform.getPlugin().log(Level.INFO, "Off-heap investigation complete: " + file);

                try {
                    resp.replyPrefixed(text("Off-heap investigation complete.", GOLD));
                    resp.replyPrefixed(text("Full report: " + file, GRAY));
                    // Only the verdict goes to chat; the rest would be dozens of lines.
                    List<String> lines = reportLines;
                    int verdictAt = lines.indexOf("--- VERDICT ---");
                    if (verdictAt >= 0) {
                        for (int i = verdictAt; i < lines.size(); i++) {
                            resp.replyPrefixed(text(lines.get(i), GRAY));
                        }
                    }
                } catch (Throwable t) {
                    platform.getPlugin().log(Level.INFO, "Report written but could not be sent to the command sender");
                }
            } catch (Throwable t) {
                this.investigation = null;
                platform.getPlugin().log(Level.WARNING, "Investigation report failed", t);
            } finally {
                executor.shutdown();
                // Only clear the field if it still refers to THIS run. Writing the report takes
                // long enough on a large process that a new investigation can be started in the
                // meantime, and clearing its handle would leave cancelInvestigation() with
                // nothing to shut down - the new scheduler thread would then outlive every
                // cancel attempt.
                if (this.investigationExecutor == executor) {
                    this.investigationExecutor = null;
                }
            }
        }, minutes, TimeUnit.MINUTES);
    }

    private void cancelInvestigation() {
        ScheduledFuture<?> task = this.investigationTask;
        if (task != null) {
            task.cancel(false);
            this.investigationTask = null;
        }
        ScheduledExecutorService executor = this.investigationExecutor;
        if (executor != null) {
            executor.shutdownNow();
            this.investigationExecutor = null;
        }
        this.investigation = null;
    }

    /** Writes a report without needing a live command sender. */
    private Path writeReportFile(SparkPlatform platform, String content) {
        try {
            Path directory = platform.getPlugin().getPluginDirectory();
            Files.createDirectories(directory);

            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

            // The same free space guard the interactive path applies, and this is the path that
            // needs it more: it runs unattended for up to twelve hours on a server already being
            // diagnosed for a resource problem, next to an incremental log that has been growing
            // for the whole run.
            long usable = directory.toFile().getUsableSpace();
            if (usable > 0 && usable < bytes.length + MIN_FREE_DISK_BYTES) {
                platform.getPlugin().log(Level.WARNING, "Not writing the investigation report: only "
                        + FormatUtil.formatBytes(usable) + " of disk space is free.");
                return java.nio.file.Paths.get("(not written - disk full)");
            }

            pruneOldReports(directory, "investigation");
            Path file = uniqueReportFile(directory, "investigation");
            Files.write(file, bytes);
            return file;
        } catch (Throwable t) {
            platform.getPlugin().log(Level.WARNING, "Unable to write investigation report", t);
            return java.nio.file.Paths.get("(write failed)"); // Path.of is Java 11; this module targets 8
        }
    }

    /**
     * Resolves a report path that nothing already holds.
     *
     * <p>{@link #FILE_STAMP} has one second resolution and {@link Files#write} truncates, so two
     * reports written in the same second silently became one - while both replies said the file
     * had been written.</p>
     */
    private static Path uniqueReportFile(Path directory, String prefix) {
        String stamp = FILE_STAMP.format(Instant.now());
        Path file = directory.resolve(prefix + "-" + stamp + ".txt");
        for (int i = 2; Files.exists(file) && i < 100; i++) {
            file = directory.resolve(prefix + "-" + stamp + "-" + i + ".txt");
        }
        return file;
    }

    private void writeDump(SparkPlatform platform, CommandResponseHandler resp) {
        ProcessMemorySnapshot s = ProcessMemorySnapshot.capture();
        StringBuilder sb = new StringBuilder();

        sb.append("spark off-heap memory report - ").append(ISO.format(Instant.ofEpochMilli(s.wallTimestamp()))).append('\n');
        sb.append("JVM: ").append(System.getProperty("java.vm.name")).append(' ')
                .append(System.getProperty("java.version")).append('\n');
        sb.append("Uptime: ")
                .append(java.lang.management.ManagementFactory.getRuntimeMXBean().getUptime() / 60_000)
                .append(" minutes\n\n");

        sb.append("== Startup arguments ==\n");
        for (String argument : java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            sb.append("  ").append(argument).append('\n');
        }
        sb.append("  MALLOC_ARENA_MAX=").append(System.getenv("MALLOC_ARENA_MAX")).append('\n');
        sb.append("  MaxDirectMemorySize=").append(DiagnosticCommand.getVmOption("MaxDirectMemorySize")).append('\n');

        sb.append("\n== Totals ==\n");
        sb.append("  rss=").append(s.rss()).append('\n');
        sb.append("  heapUsed=").append(s.heapUsed()).append(" heapCommitted=").append(s.heapCommitted())
                .append(" heapMax=").append(s.heapMax()).append('\n');
        sb.append("  nonHeapCommitted=").append(s.nonHeapCommitted()).append('\n');
        sb.append("  nioDirect=").append(s.directUsed()).append(" nettyDirect=").append(s.nettyDirect()).append('\n');
        sb.append("  threads=").append(s.threads()).append(" classes=").append(s.loadedClasses()).append('\n');
        sb.append("  unaccounted=").append(s.unaccounted()).append('\n');

        sb.append("\n== smaps_rollup ==\n");
        s.smapsRollup().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));

        sb.append("\n== cgroup ==\n");
        if (s.cgroup().isEmpty()) {
            sb.append("  unavailable\n");
        } else {
            s.cgroup().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        }

        List<ProcessMemory.Mapping> mappings = ProcessMemory.getMappings();
        sb.append("\n== Mappings ==\n");
        sb.append("  arena-shaped regions: ").append(ProcessMemory.countArenaLikeRegions(mappings)).append('\n');
        mappings.stream()
                .sorted(Comparator.comparingLong(ProcessMemory.Mapping::rss).reversed())
                .limit(40)
                .forEach(m -> sb.append("  ").append(m.rss()).append("  ").append(m.permissions())
                        .append("  ").append(m.anonymous() ? "[anonymous]" : m.path()).append('\n'));

        sb.append("\n== VM.native_memory summary ==\n").append(DiagnosticCommand.execute("VM.native_memory", "summary", "scale=MB")).append('\n');
        sb.append("\n== GC.heap_info ==\n").append(DiagnosticCommand.execute("GC.heap_info")).append('\n');
        sb.append("\n== VM.metaspace basic ==\n").append(DiagnosticCommand.execute("VM.metaspace", "basic")).append('\n');

        writeToPluginDirectory(platform, resp, "memory-report", sb.toString());
    }

    /**
     * Writes a report, keeping only the most recent {@link #MAX_REPORTS_KEPT} per prefix.
     *
     * <p>Both the pruning and the free space check exist because this writes a timestamped file
     * on every invocation, and the machine it runs on is a game host where the same operator may
     * run the command repeatedly while chasing a problem. Unbounded files in the plugin folder
     * would be a slow leak of a different kind, and filling the disk on a server that is already
     * unhealthy would turn a diagnostic into an outage.</p>
     */
    private void writeToPluginDirectory(SparkPlatform platform, CommandResponseHandler resp, String prefix, String content) {
        try {
            Path directory = platform.getPlugin().getPluginDirectory();
            Files.createDirectories(directory);

            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            long usable = directory.toFile().getUsableSpace();
            if (usable > 0 && usable < bytes.length + MIN_FREE_DISK_BYTES) {
                resp.replyPrefixed(text("Not writing a report: only "
                        + FormatUtil.formatBytes(usable) + " of disk space is free.", RED));
                return;
            }

            pruneOldReports(directory, prefix);

            Path file = uniqueReportFile(directory, prefix);
            Files.write(file, bytes);
            resp.replyPrefixed(text("Written to: ", GREEN).append(text(file.toString(), GRAY)));
        } catch (Throwable t) {
            resp.replyPrefixed(text("Unable to write the report file.", RED));
            platform.getPlugin().log(Level.WARNING, "Unable to write memory report", t);
        }
    }

    private void pruneOldReports(Path directory, String prefix) {
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            List<Path> existing = files
                    .filter(p -> p.getFileName().toString().startsWith(prefix + "-")
                            && p.getFileName().toString().endsWith(".txt"))
                    .sorted(Comparator.comparing((Path p) -> p.getFileName().toString()).reversed())
                    .collect(java.util.stream.Collectors.toList());

            for (int i = MAX_REPORTS_KEPT - 1; i < existing.size(); i++) {
                Files.deleteIfExists(existing.get(i));
            }
        } catch (IOException e) {
            // pruning is best effort - never fail the report over it
        }
    }

    // ------------------------------------------------------------------- formatting

    /**
     * Formats a possibly-negative byte count.
     *
     * <p>{@link FormatUtil#formatBytes} renders everything at or below zero as "0 bytes", which
     * is wrong here in a way that matters: 'unaccounted' is legitimately negative whenever the
     * heap is committed but not fully resident - the common case without AlwaysPreTouch - and
     * printing that as zero would hide the fact that the figure is not yet meaningful.</p>
     */
    /** Maps the -1 "unavailable" convention onto delta()'s sentinel. */
    private static long sentinel(long value) {
        return value < 0 ? Long.MIN_VALUE : value;
    }

    private static String formatSigned(long bytes) {
        if (bytes == 0) {
            return "0 bytes";
        }
        return (bytes < 0 ? "-" : "") + FormatUtil.formatBytes(Math.abs(bytes));
    }

    private static Component entry(String label, String value) {
        return text().append(text("    " + label + ": ", GRAY)).append(text(value, WHITE)).build();
    }

    private static Component delta(String label, long from, long to, long millis) {
        if (from == Long.MIN_VALUE || to == Long.MIN_VALUE) {
            return entry(label, "unavailable");
        }
        long change = to - from;
        long perHour = (long) (change / (millis / 3_600_000d));

        return text()
                .append(text("    " + label + ": ", GRAY))
                .append(text(formatSigned(from) + " -> " + formatSigned(to), WHITE))
                .append(text(" (", DARK_GRAY))
                .append(text((change >= 0 ? "+" : "-") + FormatUtil.formatBytes(Math.abs(change)),
                        change > 0 ? YELLOW : GREEN))
                .append(text(", " + (perHour >= 0 ? "+" : "-") + FormatUtil.formatBytes(Math.abs(perHour)) + "/h", DARK_GRAY))
                .append(text(")", DARK_GRAY))
                .build();
    }

}
