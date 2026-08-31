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

package me.lucko.spark.common.sampler.async;

import com.google.common.collect.ImmutableList;
import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.sampler.ThreadDumper;
import me.lucko.spark.common.sampler.async.jfr.JfrReader;
import one.profiler.AsyncProfiler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap; // fork
import java.util.List;
import java.util.Map; // fork
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Represents a profiling job within async-profiler.
 *
 * <p>Only one job can be running at a time. This is guarded by
 * {@link #createNew(AsyncProfilerAccess, AsyncProfiler)}.</p>
 */
public class AsyncProfilerJob {

    /**
     * The currently active job.
     */
    private static final AtomicReference<AsyncProfilerJob> ACTIVE = new AtomicReference<>();

    /**
     * fork - fraction of the recording discarded when deciding what counts as a leak.
     * Allocations made in the final 10% of the window are ignored, because they have not had
     * a fair chance to be freed yet and would otherwise drown out genuine slow leaks. Matches
     * async-profiler's own default for {@code jfrconv --leak}.
     */
    public static final double LEAK_TAIL_RATIO = 0.10d;

    /**
     * Ceiling on distinct addresses tracked simultaneously.
     *
     * <p>Each entry costs a HashMap.Node (32 B) + boxed Long key (16 B) + AddressState (~40 B)
     * + table slot (~10 B), so call it ~100 bytes: about 200 MB at this limit, plus a transient
     * spike while the table resizes. Entries are removed as soon as their balance settles, so
     * the map tracks genuinely outstanding memory rather than allocation history - observed peak
     * on real captures was under 10,000. The cap exists so a pathological capture degrades into
     * a partial result with a warning instead of taking the server down.</p>
     */
    private static final int MAX_TRACKED_ADDRESSES = 2_000_000;

    /**
     * Creates a new {@link AsyncProfilerJob}.
     *
     * <p>Will throw an {@link IllegalStateException} if another job is already active.</p>
     *
     * @param access the profiler access object
     * @param profiler the profiler
     * @return the job
     */
    static AsyncProfilerJob createNew(AsyncProfilerAccess access, AsyncProfiler profiler) {
        AsyncProfilerJob job = new AsyncProfilerJob(access, profiler);
        if (!ACTIVE.compareAndSet(null, job)) {
            throw new IllegalStateException("Another profiler is already active: " + ACTIVE.get());
        }
        return job;
    }

    public static AsyncProfilerJob getActiveJob() {
        return ACTIVE.get();
    }

    /** The async-profiler access object */
    private final AsyncProfilerAccess access;
    /** The async-profiler instance */
    private final AsyncProfiler profiler;

    // Set on init
    /** The platform */
    private SparkPlatform platform;
    /** The sample collector */
    private SampleCollector<?> sampleCollector;
    /** fork - additional collectors captured in the same recording */
    private List<SampleCollector<?>> extraCollectors = ImmutableList.of();
    /** The thread dumper */
    private ThreadDumper threadDumper;
    /** The profiling window */
    private int window;
    /** If the profiler should run in quiet mode */
    private boolean quiet;
    /** If the profiler needs to use the same clock as {@link System#nanoTime()} */
    private boolean forceNanoTime;

    /** The file used by async-profiler to output data */
    private Path outputFile;

    private AsyncProfilerJob(AsyncProfilerAccess access, AsyncProfiler profiler) {
        this.access = access;
        this.profiler = profiler;
    }

    /**
     * Executes an async-profiler command.
     *
     * @param command the command
     * @return the output
     */
    private String execute(Collection<String> command) {
        try {
            return this.profiler.execute(String.join(",", command));
        } catch (IOException e) {
            throw new RuntimeException("Exception whilst executing profiler command", e);
        }
    }

    /**
     * fork - folds an additional collector's arguments into the start command.
     *
     * <p>Additional collectors may only contribute additive engine arguments. async-profiler's
     * {@code event=} is single-valued: a second one replaces the first, so an extra collector
     * emitting one would silently switch the primary collector off while the profile still
     * reports that it contains that collector's data. There is no way to notice that from the
     * result, so the only non-misleading failure mode is to refuse to start.</p>
     *
     * @param command the command being built
     * @param arguments the additional collector's arguments
     */
    static void addExtraCollectorArguments(ImmutableList.Builder<String> command, Collection<String> arguments) {
        for (String argument : arguments) {
            if (argument.startsWith("event=")) {
                throw new IllegalStateException("An additional sample collector specified '" + argument +
                        "', but 'event=' is single-valued in async-profiler and would override the primary collector.");
            }
            command.add(argument);
        }
    }

    /**
     * Checks to ensure that this job is still active.
     */
    private void checkActive() {
        if (ACTIVE.get() != this) {
            throw new IllegalStateException("Profiler job no longer active!");
        }
    }

    // Initialise the job
    public void init(SparkPlatform platform, SampleCollector<?> collector, ThreadDumper threadDumper, int window, boolean quiet, boolean forceNanoTime) {
        init(platform, collector, ImmutableList.of(), threadDumper, window, quiet, forceNanoTime);
    }

    // fork - initialise with additional collectors running in the SAME recording.
    //
    // async-profiler's 'event=' is single-valued, but 'alloc=' and 'nativemem=' are separate
    // additive engines, so one recording can legitimately carry execution samples, live-object
    // allocation samples and malloc/free events at once. Verified empirically against
    // async-profiler 4.5: a single run with "event=wall,interval=10ms,alloc=512k,live,nativemem"
    // produced profiler.WallClockSample, profiler.Malloc, profiler.Free, profiler.LiveObject
    // and jdk.ObjectAllocationInNewTLAB in one JFR, and the native leak totals matched a
    // nativemem-only run of the same workload to within 4%.
    //
    // This is what lets the fork answer CPU, native-memory and heap-leak questions from ONE
    // command and ONE share link, instead of asking the user to profile three times.
    public void init(SparkPlatform platform, SampleCollector<?> collector, List<SampleCollector<?>> extraCollectors, ThreadDumper threadDumper, int window, boolean quiet, boolean forceNanoTime) {
        this.platform = platform;
        this.sampleCollector = collector;
        this.extraCollectors = extraCollectors;
        this.threadDumper = threadDumper;
        this.window = window;
        this.quiet = quiet;
        this.forceNanoTime = forceNanoTime;
    }

    public SparkPlatform getPlatform() {
        return this.platform;
    }

    /**
     * Starts the job.
     */
    public void start() {
        checkActive();

        try {
            // create a new temporary output file
            try {
                this.outputFile = this.platform.getTemporaryFiles().create("spark-", "-profile-data.jfr.tmp");
            } catch (IOException e) {
                throw new RuntimeException("Unable to create temporary output file", e);
            }

            // construct a command to send to async-profiler
            ImmutableList.Builder<String> command = ImmutableList.<String>builder()
                    .add("start")
                    .addAll(this.sampleCollector.initArguments(this.access));

            // fork - fold in the arguments of any additional collectors so all of them are
            // captured by the same recording. The extra collectors deliberately contribute only
            // additive engine arguments ('alloc=', 'nativemem='), never another 'event=', because
            // 'event=' is single-valued and a second one would silently replace the primary
            // collector's. They can still collide on 'alloc=', which is why SamplerBuilder
            // refuses --alloc combined with --heap-leaks: silently overriding the user's
            // --interval would be worse than refusing.
            for (SampleCollector<?> extra : this.extraCollectors) {
                addExtraCollectorArguments(command, extra.initArguments(this.access));
            }

            command.add("threads").add("jfr").add("file=" + this.outputFile.toString());

            if (this.quiet) {
                command.add("loglevel=NONE");
            }
            if (this.threadDumper instanceof ThreadDumper.Specific) {
                command.add("filter");
            }
            if (this.forceNanoTime) {
                command.add("clock=monotonic");
            }

            // start the profiler
            String resp = execute(command.build()).trim();

            if (!resp.equalsIgnoreCase("profiling started")) {
                throw new RuntimeException("Unexpected response: " + resp);
            }

            // append threads to be profiled, if necessary
            if (this.threadDumper instanceof ThreadDumper.Specific) {
                ThreadDumper.Specific threadDumper = (ThreadDumper.Specific) this.threadDumper;
                for (Thread thread : threadDumper.getThreads()) {
                    this.profiler.addThread(thread);
                }
            }

        } catch (Exception e) {
            try {
                this.profiler.stop();
            } catch (Exception e2) {
                // ignore
            }
            close();

            throw e;
        }
    }

    /**
     * Stops the job.
     */
    public void stop() {
        checkActive();

        try {
            this.profiler.stop();
        } catch (IllegalStateException e) {
            if (!e.getMessage().equals("Profiler is not active")) { // ignore
                throw e;
            }
        } finally {
            close();
        }
    }

    /**
     * fork - aggregates the collected data, routing each collector's events into its own
     * aggregator.
     *
     * <p>The JFR is opened once and read per collector rather than once overall, because
     * {@link JfrReader#readAllEvents(Class)} filters by event class and the three streams are
     * genuinely independent trees - mixing them into one aggregator would produce a profile
     * where CPU milliseconds and leaked bytes are summed into the same meaningless number.</p>
     */
    public void aggregate(AsyncDataAggregator dataAggregator, Map<SampleCollector<?>, AsyncDataAggregator> extraAggregators) {
        // read the jfr file produced by async-profiler
        //
        // ONE READER PER COLLECTOR, and that is not an oversight to tidy up later. A JfrReader
        // is a forward-only cursor over the file: readAllEvents() and the streaming loop both
        // consume to EOF. Sharing a single reader across collectors means the first one drains
        // the file and every subsequent collector silently reads ZERO events - so a --leaks
        // profile would upload with an execution tree and completely empty leak trees, while
        // still reporting has_native_memory=true. "No leaks found" on a server that is actively
        // leaking is the worst possible failure mode, because it looks like an answer.
        //
        // Verified: with a shared reader, the second collector saw 0 of 733,251 malloc events.
        // Re-opening costs a file handle and a re-parse, which is nothing next to being wrong.
        // fork - in a finally block, not at the end. A leak recording is written with
        // 'nativemem=0' (every malloc) and routinely runs to gigabytes; leaving one behind on a
        // parse failure would fill the disk of the server being diagnosed.
        try {
            try (JfrReader reader = new JfrReader(this.outputFile)) {
                readSegments(reader, this.sampleCollector, dataAggregator);
            } catch (Exception e) {
                throw wrapParsingException(e);
            }

            for (Map.Entry<SampleCollector<?>, AsyncDataAggregator> entry : extraAggregators.entrySet()) {
                SampleCollector<?> collector = entry.getKey();
                try (JfrReader reader = new JfrReader(this.outputFile)) {
                    if (collector instanceof SampleCollector.NativeMemory) {
                        readNativeMemoryLeakSegments(reader, (SampleCollector.NativeMemory) collector, entry.getValue());
                    } else {
                        readSegments(reader, collector, entry.getValue());
                    }
                } catch (Exception e) {
                    throw wrapParsingException(e);
                }
            }
        } finally {
            deleteOutputFile();
        }
    }

    private RuntimeException wrapParsingException(Exception e) {
        boolean fileExists;
        try {
            fileExists = Files.exists(this.outputFile) && Files.size(this.outputFile) != 0;
        } catch (IOException ex) {
            fileExists = false;
        }

        if (fileExists) {
            return new JfrParsingException("Error parsing JFR data from profiler output", e);
        } else {
            return new JfrParsingException("Error parsing JFR data from profiler output - file " + this.outputFile + " does not exist!", e);
        }
    }

    public void deleteOutputFile() {
        try {
            Files.deleteIfExists(this.outputFile);
        } catch (IOException e) {
            // ignore
        }
    }

    private <E extends JfrReader.Event> void readSegments(JfrReader reader, SampleCollector<E> collector, AsyncDataAggregator dataAggregator) throws IOException {
        boolean threadScoped = collector.isThreadScoped();
        List<E> samples = reader.readAllEvents(collector.eventClass());
        for (E sample : samples) {
            String threadName = reader.threads.get((long) sample.tid);
            if (threadName == null) {
                // A thread the recording never named. For an execution profile there is nothing
                // useful to show, but for a leak the bytes still count - and native allocations
                // are routinely made on threads that never appear in the Java thread pool.
                if (threadScoped) {
                    continue;
                }
                threadName = "unknown thread #" + sample.tid;
            }

            if (threadScoped && !this.threadDumper.isThreadIncluded(sample.tid, threadName)) {
                continue;
            }

            long value = collector.measure(sample);

            // parse the segment and give it to the data aggregator
            ProfileSegment segment = ProfileSegment.parseSegment(reader, sample, threadName, value);
            dataAggregator.insertData(segment, this.window);
        }
    }

    /**
     * fork - aggregates native memory LEAKS rather than raw allocations.
     *
     * <p>Every other collector can measure an event in isolation. This one cannot: a single
     * malloc tells you nothing, because the overwhelming majority of native allocations are
     * freed almost immediately and are perfectly healthy. What matters is the allocations with
     * no matching free by the end of the recording.</p>
     *
     * <p>async-profiler emits a {@code profiler.Malloc} event carrying an address and size, and
     * a {@code profiler.Free} event carrying the same address with {@code size == 0}. Both
     * arrive through {@link JfrReader.MallocEvent}. So the correlation is: index live
     * allocations by address, drop them when the matching free shows up, and whatever is left
     * standing is the leak. This mirrors what async-profiler's own {@code jfrconv --leak} does
     * in {@code MallocLeakAggregator}, reimplemented here so a profile can be produced and
     * uploaded in one step with no external conversion tool on the server.</p>
     *
     * <p>The tail cutoff matters more than it looks. Without it, everything allocated in the
     * final moments of the recording is reported as leaked purely because the program had not
     * got round to freeing it yet - which would bury a real slow leak under a mountain of
     * perfectly normal short-lived allocations. Discarding the last portion of the window is
     * the same correction async-profiler applies by default.</p>
     *
     * <p>Freeing an address that was never seen being allocated is normal and harmless - it
     * happens for anything allocated before profiling began - and is simply ignored.</p>
     */
    private void readNativeMemoryLeakSegments(JfrReader reader, SampleCollector.NativeMemory collector, AsyncDataAggregator dataAggregator) throws IOException {
        // ORDER-INDEPENDENT NET COUNTING. Do not reintroduce sorting here.
        //
        // Two earlier attempts got this wrong and both are worth recording:
        //
        // 1. readAllEvents() + correlate. Materialises every event and sorts it. Measured at
        //    ~23 MB/s of JFR on a busy server, that is ~123 million events for a one-hour
        //    capture - about 5.5 GB of heap, allocated inside the server at /spark profiler
        //    stop. It OOMs the server.
        //
        // 2. Streaming with a bounded sort window. The premise was that events arrive nearly
        //    sorted. They do not. async-profiler buffers per thread and flushes at dump, so a
        //    low-traffic thread's EARLIEST events land at the very END of the file. Measured on
        //    a realistic multi-threaded capture: 66% of events out of order, maximum backward
        //    displacement 6,079,248 events. No practical window covers that, and when the window
        //    is overrun a free is processed before its malloc, producing a phantom leak.
        //
        // The fix is to stop needing order at all. Whether an allocation leaked is just
        // "was this address malloc'd more times than it was freed" - a net count, which is
        // commutative and therefore immune to arrival order. Memory scales with DISTINCT
        // ADDRESSES rather than total events, which on a leaking server is bounded by the
        // outstanding allocation set.
        //
        // Address reuse is handled correctly: malloc(X), free(X), malloc(X) nets to +1, one
        // leak, and the retained details are the most recent malloc - the one still outstanding.
        long firstTime = Long.MAX_VALUE;
        long lastTime = Long.MIN_VALUE;
        Map<Long, AddressState> states = new HashMap<>();
        boolean capped = false;

        for (JfrReader.MallocEvent event; (event = reader.readEvent(JfrReader.MallocEvent.class)) != null; ) {
            if (event.time < firstTime) {
                firstTime = event.time;
            }
            if (event.time > lastTime) {
                lastTime = event.time;
            }

            capped |= !track(states, event);
        }

        if (capped) {
            this.platform.getPlugin().log(Level.WARNING,
                    "Native memory leak tracking hit its " + MAX_TRACKED_ADDRESSES + " address cap. " +
                    "Results are a partial view - profile for a shorter period for a complete one.");
        }

        if (states.isEmpty()) {
            return;
        }

        // allocations made right at the end have not had a fair chance to be freed yet
        long span = lastTime - firstTime;
        long cutoff = span > 0
                ? lastTime - (long) (span * LEAK_TAIL_RATIO)
                : Long.MAX_VALUE;

        for (AddressState st : states.values()) {
            if (st.balance <= 0 || st.time == 0L) {
                continue; // net-freed, or we only ever saw frees for this address
            }
            if (st.time > cutoff) {
                continue;
            }

            // Deliberately not thread-filtered, and unnamed threads are kept. The default
            // dumper on most platforms is the server thread alone, so filtering here discarded
            // every leak allocated anywhere else and reported zero leaked bytes next to
            // has_native_memory = true - a result no reader can tell from "found nothing".
            String threadName = reader.threads.get((long) st.tid);
            if (threadName == null) {
                threadName = "unknown thread #" + st.tid;
            }

            JfrReader.MallocEvent event = new JfrReader.MallocEvent(st.time, st.tid, st.stackTraceId, 0L, st.size);
            ProfileSegment segment = ProfileSegment.parseSegment(reader, event, threadName, st.size);
            dataAggregator.insertData(segment, this.window);
        }
    }

    /**
     * fork - applies one malloc or free event to the running net-count state.
     *
     * <p>An address whose balance returns to zero is dropped immediately. That is what keeps the
     * map bounded by memory which is genuinely still outstanding, rather than by every address the
     * process has ever touched: the overwhelming majority of allocations are freed promptly, and
     * retaining their (by then meaningless) entries would grow the map with total allocation
     * history - the precise unbounded growth this whole design exists to avoid.</p>
     *
     * <p>Dropping the details along with the entry is safe. A later malloc of the same address
     * simply starts a fresh entry - which is also what makes address reuse come out right, since
     * the retained details are always those of the allocation that is still outstanding. A later
     * unmatched free starts one at -1, which is filtered out when the leaks are reported.</p>
     *
     * @return false if the address cap was hit and the event had to be dropped
     */
    static boolean track(Map<Long, AddressState> states, JfrReader.MallocEvent event) {
        AddressState st = states.get(event.address);
        if (st == null) {
            if (states.size() >= MAX_TRACKED_ADDRESSES) {
                return false;
            }
            st = new AddressState();
            states.put(event.address, st);
        }

        if (event.size == 0) {
            // a free. If this is the first we have seen of the address, the entry created above
            // records the debt so the malloc cancels out when it arrives (async-profiler flushes
            // per thread, so a free can genuinely be read before its own malloc) rather than
            // being counted as a leak.
            st.balance--;
        } else {
            st.balance++;
            st.time = event.time;
            st.tid = event.tid;
            st.stackTraceId = event.stackTraceId;
            st.size = event.size;
        }

        if (st.balance == 0) {
            states.remove(event.address);
        }
        return true;
    }

    /**
     * Net malloc/free balance for one address, plus the details of the outstanding allocation.
     * Mutable and reused rather than reallocated per event - this map can hold millions of
     * entries and per-entry cost is what decides whether leak tracking fits in a server's heap.
     */
    static final class AddressState {
        int balance;
        long time;
        int tid;
        int stackTraceId;
        long size;
    }

    public int getWindow() {
        return this.window;
    }

    private void close() {
        ACTIVE.compareAndSet(this, null);
    }
}
