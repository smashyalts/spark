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

import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.monitor.memory.ProcessMemorySnapshot; // fork
import me.lucko.spark.common.platform.PlatformInfo;
import me.lucko.spark.common.sampler.AbstractSampler;
import me.lucko.spark.common.sampler.SamplerMode;
import me.lucko.spark.common.sampler.SamplerSettings;
import me.lucko.spark.common.sampler.SamplerType;
import me.lucko.spark.common.sampler.window.ProfilingWindowUtils;
import me.lucko.spark.common.tick.TickHook;
import me.lucko.spark.common.util.SparkScheduledThreadPoolExecutor;
import me.lucko.spark.common.util.SparkThreadFactory;
import me.lucko.spark.common.util.TimeUtil;
import me.lucko.spark.common.ws.SamplerViewerSocket;
import me.lucko.spark.proto.SparkSamplerProtos;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerData;

import com.google.common.collect.ImmutableList; // fork
import java.util.LinkedHashMap; // fork
import java.util.Locale;
import java.util.Map; // fork
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.logging.Level;

/**
 * A sampler implementation using async-profiler.
 */
public class AsyncSampler extends AbstractSampler {

    /** fork - version stamped into every profile so a reader knows which build produced it */
    public static final String FORK_VERSION = "spark-nativemem-1.0";

    /** Function to collect and measure samples - either execution or allocation */
    private final SampleCollector<?> sampleCollector;

    /** Object that provides access to the async-profiler API */
    private final AsyncProfilerAccess profilerAccess;

    /** Responsible for aggregating and then outputting collected sampling data */
    private final AsyncDataAggregator dataAggregator;

    /**
     * fork - additional collectors captured by the same recording, each with its own
     * aggregator. Keyed by collector so {@link AsyncProfilerJob#aggregate} can route events
     * from the shared JFR into the right tree.
     */
    private final Map<SampleCollector<?>, AsyncDataAggregator> extraAggregators = new LinkedHashMap<>();

    /** fork - thread grouper, retained so extra aggregators are grouped identically */
    private final me.lucko.spark.common.sampler.ThreadGrouper threadGrouper;

    /** Whether to force the sampler to use monotonic/nano time */
    private final boolean forceNanoTime;

    /** Mutex for the current profiler job */
    private final Object[] currentJobMutex = new Object[0];

    /** Current profiler job */
    private AsyncProfilerJob currentJob;

    /** The executor used for scheduling and management */
    private ScheduledExecutorService scheduler;

    /** The task to send statistics to the viewer socket */
    private ScheduledFuture<?> socketStatisticsTask;

    public AsyncSampler(SparkPlatform platform, SamplerSettings settings, SampleCollector<?> collector) {
        this(platform, settings, collector, new AsyncDataAggregator(settings.threadGrouper(), settings.ignoreSleeping()), false);
    }

    public AsyncSampler(SparkPlatform platform, SamplerSettings settings, SampleCollector<?> collector, int tickLengthThreshold) {
        this(platform, settings, collector, new TickedAsyncDataAggregator(settings.threadGrouper(), settings.ignoreSleeping(), platform.getTickReporter(), tickLengthThreshold), true);
    }

    private AsyncSampler(SparkPlatform platform, SamplerSettings settings, SampleCollector<?> collector, AsyncDataAggregator dataAggregator, boolean forceNanoTime) {
        super(platform, settings);
        this.sampleCollector = collector;
        this.dataAggregator = dataAggregator;
        this.threadGrouper = settings.threadGrouper(); // fork
        this.forceNanoTime = forceNanoTime;
        this.profilerAccess = AsyncProfilerAccess.getInstance(platform);
        this.scheduler = new SparkScheduledThreadPoolExecutor(1, new SparkThreadFactory("spark-async-sampler-worker", false));
    }

    /**
     * fork - registers an additional collector to be captured by the same recording.
     *
     * <p>Must be called before {@link #start()}. Each collector gets its own aggregator so the
     * resulting trees stay separate - CPU milliseconds and leaked bytes are not commensurable
     * and must never be summed into one tree.</p>
     */
    public void addExtraCollector(SampleCollector<?> collector) {
        if (this.startTime != -1) {
            throw new IllegalStateException("Sampler already started");
        }
        this.extraAggregators.put(collector, new AsyncDataAggregator(this.threadGrouper, false));
    }

    /**
     * Starts the profiler.
     */
    @Override
    public void start() {
        checkAlreadyRunning();
        super.start();

        TickHook tickHook = this.platform.getTickHook();
        if (tickHook != null) {
            this.windowStatisticsCollector.startCountingTicks(tickHook);
        }

        int window = ProfilingWindowUtils.monotonicTimeToWindow(this.startTime);

        AsyncProfilerJob job = this.profilerAccess.startNewProfilerJob();
        job.init(this.platform, this.sampleCollector, ImmutableList.copyOf(this.extraAggregators.keySet()), this.threadDumper, window, this.background, this.forceNanoTime);
        job.start();
        this.windowStatisticsCollector.recordWindowStartTime(window);
        this.currentJob = job;

        // rotate the sampler job to put data into a new window
        boolean shouldNotRotate = this.sampleCollector instanceof SampleCollector.Allocation && ((SampleCollector.Allocation) this.sampleCollector).isLiveOnly();

        // fork - window rotation stops and restarts async-profiler, which throws away the
        // in-flight malloc->free correlation and the live-object set. For leak detection that
        // is fatal: a leak is by definition an allocation that outlives the window it was made
        // in, so rotating every 60s would report almost everything as leaked and simultaneously
        // lose the long-lived allocations that actually matter. Leak modes therefore run as one
        // continuous recording, exactly as upstream already does for --alloc-live-only.
        for (SampleCollector<?> extra : this.extraAggregators.keySet()) {
            if (extra instanceof SampleCollector.NativeMemory || isHeapLeakCollector(extra)) {
                shouldNotRotate = true;
            }
        }
        if (!shouldNotRotate) {
            this.scheduler.scheduleAtFixedRate(
                    this::rotateProfilerJob,
                    ProfilingWindowUtils.WINDOW_SIZE_SECONDS,
                    ProfilingWindowUtils.WINDOW_SIZE_SECONDS,
                    TimeUnit.SECONDS
            );
        }

        recordInitialGcStats();
        scheduleTimeout();
    }

    private void rotateProfilerJob() {
        try {
            synchronized (this.currentJobMutex) {
                AsyncProfilerJob previousJob = this.currentJob;
                if (previousJob == null) {
                    return;
                }

                try {
                    // stop the previous job
                    previousJob.stop();
                } catch (Exception e) {
                    this.platform.getPlugin().log(Level.WARNING, "Failed to stop previous profiler job", e);
                }

                // start a new job
                int window = previousJob.getWindow() + 1;
                AsyncProfilerJob newJob = this.profilerAccess.startNewProfilerJob();
                newJob.init(this.platform, this.sampleCollector, this.threadDumper, window, this.background, this.forceNanoTime);
                newJob.start();
                this.windowStatisticsCollector.recordWindowStartTime(window);
                this.currentJob = newJob;

                // collect statistics for the previous window
                try {
                    this.windowStatisticsCollector.measureNow(previousJob.getWindow());
                } catch (Exception e) {
                    this.platform.getPlugin().log(Level.WARNING, "Failed to measure window statistics", e);
                }

                // aggregate the output of the previous job
                previousJob.aggregate(this.dataAggregator, this.extraAggregators); // fork

                // prune data older than the history size
                IntPredicate predicate = ProfilingWindowUtils.keepHistoryBefore(window);
                this.dataAggregator.pruneData(predicate);
                this.windowStatisticsCollector.pruneStatistics(predicate);

                this.scheduler.execute(this::processWindowRotate);
            }
        } catch (Throwable e) {
            this.platform.getPlugin().log(Level.WARNING, "Exception occurred while rotating profiler job", e);
        }
    }

    private void scheduleTimeout() {
        if (this.autoEndTime == -1) {
            return;
        }

        long delay = this.autoEndTime - TimeUtil.monotonicCurrentTimeMillis();
        if (delay <= 0) {
            return;
        }

        this.scheduler.schedule(() -> {
            try {
                stop(false);
                this.future.complete(this);
            } catch (Exception e) {
                this.future.completeExceptionally(e);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void checkAlreadyRunning() {
        AsyncProfilerJob activeJob = AsyncProfilerJob.getActiveJob();
        if (activeJob == null) {
            return;
        }

        SparkPlatform activeJobPlatform = activeJob.getPlatform();
        if (activeJobPlatform != null && activeJobPlatform != this.platform) {
            PlatformInfo.Type thisPlatformType = this.platform.getPlugin().getPlatformInfo().getType();
            PlatformInfo.Type activePlatformType = activeJobPlatform.getPlugin().getPlatformInfo().getType();

            if (thisPlatformType != activePlatformType) {
                throw new UnsupportedOperationException(
                        "A profiler is already running on the " + activePlatformType.name().toLowerCase(Locale.ROOT) + " side. " +
                        "You need to stop it (using /" + activeJobPlatform.getPlugin().getCommandName() + " profiler cancel) " +
                        "before you can start one on the " + thisPlatformType.name().toLowerCase(Locale.ROOT) + " side."
                );
            }
        }

        throw new UnsupportedOperationException("A profiler is already running.");
    }

    /**
     * Stops the profiler.
     */
    @Override
    public void stop(boolean cancelled) {
        super.stop(cancelled);

        synchronized (this.currentJobMutex) {
            this.currentJob.stop();
            if (!cancelled) {
                this.windowStatisticsCollector.measureNow(this.currentJob.getWindow());
                this.currentJob.aggregate(this.dataAggregator, this.extraAggregators); // fork
            } else {
                this.currentJob.deleteOutputFile();
            }
            this.currentJob = null;
        }

        if (this.socketStatisticsTask != null) {
            this.socketStatisticsTask.cancel(false);
        }

        if (this.scheduler != null) {
            this.scheduler.shutdown();
            this.scheduler = null;
        }
        this.dataAggregator.close();
    }

    @Override
    public void attachSocket(SamplerViewerSocket socket) {
        super.attachSocket(socket);

        if (this.socketStatisticsTask == null) {
            this.socketStatisticsTask = this.scheduler.scheduleAtFixedRate(this::sendStatisticsToSocket, 10, 10, TimeUnit.SECONDS);
        }
    }

    @Override
    public SamplerType getType() {
        return SamplerType.ASYNC;
    }

    @Override
    public String getLibraryVersion() {
        return this.profilerAccess.getVersion();
    }

    @Override
    public SamplerMode getMode() {
        return this.sampleCollector.getMode();
    }

    @Override
    public SamplerData toProto(SparkPlatform platform, ExportProps exportProps) {
        SamplerData.Builder proto = SamplerData.newBuilder();
        if (exportProps.channelInfo() != null) {
            proto.setChannelInfo(exportProps.channelInfo());
        }
        writeMetadataToProto(proto, platform, exportProps.creator(), exportProps.comment(), this.dataAggregator);
        writeDataToProto(proto, this.dataAggregator, AsyncNodeExporter::new, exportProps.classSourceLookup().get(), platform::createClassFinder);
        writeExtendedDataToProto(proto); // fork
        return proto.build();
    }

    /**
     * fork - writes the extra leak trees and a summary of what this profile actually contains.
     *
     * <p>The summary matters as much as the data. Without it a reader cannot distinguish "native
     * memory profiling ran and found nothing" from "native memory profiling was never enabled",
     * and reporting a clean bill of health for a profile that never looked would be worse than
     * reporting nothing at all.</p>
     */
    private void writeExtendedDataToProto(SamplerData.Builder proto) {
        SparkSamplerProtos.ExtendedProfileContents.Builder contents = SparkSamplerProtos.ExtendedProfileContents.newBuilder()
                .setHasExecution(true)
                .setLeakTailRatio(AsyncProfilerJob.LEAK_TAIL_RATIO)
                .setForkVersion(FORK_VERSION);

        long duration = this.startTime == -1 ? 0 : System.currentTimeMillis() - this.startTime;
        contents.setDurationMillis(duration);

        // fork - process-level memory accounting on every export, memory profile or not.
        // Cheap (no smaps parse), and it is what makes a leak total interpretable: the same
        // number means something completely different depending on how much of the process's
        // resident memory the JVM can already account for.
        // fork - guarded. This runs on EVERY profile export, including plain CPU profiles that
        // have nothing to do with memory, so a failure here must cost the memory section and
        // nothing else. Losing an hour-long profiling run because /proc looked unfamiliar, or an
        // MBean behaved oddly on an unusual JVM, would be a far worse outcome than the missing
        // accounting - and the analysis side already handles its absence.
        try {
            proto.setProcessMemory(ProcessMemorySnapshot.capture().toProto());
            contents.setHasProcessMemory(true);
        } catch (Throwable t) {
            this.platform.getPlugin().log(Level.WARNING, "Unable to capture process memory accounting", t);
        }

        for (Map.Entry<SampleCollector<?>, AsyncDataAggregator> entry : this.extraAggregators.entrySet()) {
            SampleCollector<?> collector = entry.getKey();

            if (collector instanceof SampleCollector.NativeMemory) {
                long total = writeExtraDataToProto(entry.getValue(), AsyncNodeExporter::new, proto::addNativeMemoryThreads);
                contents.setHasNativeMemory(true);
                contents.setNativeMemoryLeakedBytes(total);
            } else if (isHeapLeakCollector(collector)) {
                long total = writeExtraDataToProto(entry.getValue(), AsyncNodeExporter::new, proto::addHeapLeakThreads);
                contents.setHasHeapLeak(true);
                contents.setHeapLeakedBytes(total);
            }
        }

        proto.setExtendedContents(contents);
    }

    /**
     * fork - identifies a collector whose data belongs in the heap-leak trees.
     *
     * <p>This is a named helper rather than an inline {@code instanceof} because getting it
     * wrong is silent. {@link SampleCollector.HeapLeak} is a sibling of
     * {@link SampleCollector.Allocation}, not a subclass of it - they read different JFR event
     * streams ({@code profiler.LiveObject} vs {@code jdk.ObjectAllocationInNewTLAB}), so making
     * one extend the other would inherit the wrong {@link SampleCollector#eventClass()}. An
     * {@code instanceof Allocation} test therefore never matches the collector that
     * {@code --heap-leaks} actually registers, and the failure mode is a profile that uploads
     * successfully with {@code has_heap_leak = false} - indistinguishable, to any reader, from a
     * run where heap leak detection was never switched on.</p>
     *
     * <p>Live-only {@link SampleCollector.Allocation} is included because it is the same
     * question asked through the older flag: {@code alloc} plus {@code live} is retention, not
     * allocation rate.</p>
     */
    static boolean isHeapLeakCollector(SampleCollector<?> collector) {
        return collector instanceof SampleCollector.HeapLeak
                || (collector instanceof SampleCollector.Allocation && ((SampleCollector.Allocation) collector).isLiveOnly());
    }

}
