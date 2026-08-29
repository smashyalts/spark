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

package me.lucko.spark.common.sampler;

import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.sampler.async.AsyncProfilerAccess;
import me.lucko.spark.common.sampler.async.AsyncSampler;
import me.lucko.spark.common.sampler.async.SampleCollector;
import me.lucko.spark.common.sampler.java.JavaSampler;
import me.lucko.spark.common.tick.TickHook;
import me.lucko.spark.common.util.TimeUtil;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Builds {@link Sampler} instances.
 */
@SuppressWarnings("UnusedReturnValue")
public class SamplerBuilder {

    private SamplerMode mode = SamplerMode.EXECUTION;
    private double samplingInterval = -1;
    private boolean ignoreSleeping = false;
    private boolean forceJavaSampler = false;
    private boolean allocLiveOnly = false;
    private long autoEndTime = -1;
    private boolean background = false;
    private ThreadDumper threadDumper = ThreadDumper.ALL;
    private Supplier<ThreadGrouper> threadGrouper = ThreadGrouper.BY_NAME;

    private int ticksOver = -1;
    private TickHook tickHook = null;

    // fork - additional leak-detection engines captured in the same recording
    private boolean nativeMemoryLeaks = false;
    private boolean heapLeaks = false;

    public SamplerBuilder() {
    }

    public SamplerBuilder mode(SamplerMode mode) {
        this.mode = mode;
        return this;
    }

    /** fork - also detect native (off-heap) memory leaks in the same recording */
    public SamplerBuilder nativeMemoryLeaks(boolean nativeMemoryLeaks) {
        this.nativeMemoryLeaks = nativeMemoryLeaks;
        return this;
    }

    /** fork - also detect Java heap leaks (live objects) in the same recording */
    public SamplerBuilder heapLeaks(boolean heapLeaks) {
        this.heapLeaks = heapLeaks;
        return this;
    }

    public SamplerBuilder samplingInterval(double samplingInterval) {
        this.samplingInterval = samplingInterval;
        return this;
    }

    public SamplerBuilder completeAfter(long timeout, TimeUnit unit) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("timeout > 0");
        }
        this.autoEndTime = TimeUtil.monotonicCurrentTimeMillis() + unit.toMillis(timeout);
        return this;
    }

    public SamplerBuilder background(boolean background) {
        this.background = background;
        return this;
    }

    public SamplerBuilder threadDumper(ThreadDumper threadDumper) {
        this.threadDumper = threadDumper;
        return this;
    }

    public SamplerBuilder threadGrouper(Supplier<ThreadGrouper> threadGrouper) {
        this.threadGrouper = threadGrouper;
        return this;
    }

    public SamplerBuilder ticksOver(int ticksOver, TickHook tickHook) {
        this.ticksOver = ticksOver;
        this.tickHook = tickHook;
        return this;
    }

    public SamplerBuilder ignoreSleeping(boolean ignoreSleeping) {
        this.ignoreSleeping = ignoreSleeping;
        return this;
    }

    public SamplerBuilder forceJavaSampler(boolean forceJavaSampler) {
        this.forceJavaSampler = forceJavaSampler;
        return this;
    }

    public SamplerBuilder allocLiveOnly(boolean allocLiveOnly) {
        this.allocLiveOnly = allocLiveOnly;
        return this;
    }

    public Sampler start(SparkPlatform platform) throws UnsupportedOperationException {
        if (this.samplingInterval <= 0) {
            throw new IllegalArgumentException("samplingInterval = " + this.samplingInterval);
        }

        AsyncProfilerAccess asyncProfiler = AsyncProfilerAccess.getInstance(platform);

        boolean onlyTicksOverMode = this.ticksOver != -1 && this.tickHook != null;
        boolean canUseAsyncProfiler = asyncProfiler.checkSupported(platform) && (!onlyTicksOverMode || platform.getTickReporter() != null);

        if (this.mode == SamplerMode.ALLOCATION) {
            if (!canUseAsyncProfiler || !asyncProfiler.checkAllocationProfilingSupported(platform)) {
                throw new UnsupportedOperationException("Allocation profiling is not supported on your system. Check the console for more info.");
            }
            if (this.ignoreSleeping) {
                platform.getPlugin().log(Level.WARNING, "Ignoring sleeping threads is not supported in allocation profiling mode. Sleeping threads will be included in the results.");
            }
        }

        if (this.forceJavaSampler) {
            canUseAsyncProfiler = false;
        }

        int interval = (int) (this.mode == SamplerMode.EXECUTION ?
                this.samplingInterval * 1000d : // convert to microseconds
                this.samplingInterval
        );

        SamplerSettings settings = new SamplerSettings(interval, this.threadDumper, this.threadGrouper.get(), this.autoEndTime, this.background, this.ignoreSleeping);

        // fork - leak detection requires async-profiler; there is no Java-engine equivalent,
        // so fail loudly rather than silently producing a profile with no leak data in it.
        boolean wantsLeakDetection = this.nativeMemoryLeaks || this.heapLeaks;
        if (wantsLeakDetection) {
            // Check forceJavaSampler separately: it clears canUseAsyncProfiler above, so folding
            // the two together tells a user who passed --force-java-sampler that async-profiler
            // is "not available on your system" and to check a console that says nothing.
            if (this.forceJavaSampler) {
                throw new UnsupportedOperationException("Leak detection cannot be used with --force-java-sampler: it is implemented by async-profiler and has no Java-engine equivalent.");
            }
            if (!canUseAsyncProfiler) {
                throw new UnsupportedOperationException("Leak detection requires the async-profiler engine, which is not available on your system. Check the console for more info.");
            }
            if (this.nativeMemoryLeaks && !asyncProfiler.checkNativeMemoryProfilingSupported(platform)) {
                throw new UnsupportedOperationException("Native memory leak detection is not supported on your system. Check the console for more info.");
            }
            if (this.heapLeaks && !asyncProfiler.checkAllocationProfilingSupported(platform)) {
                throw new UnsupportedOperationException("Heap leak detection is not supported on your system. Check the console for more info.");
            }
            if (onlyTicksOverMode) {
                throw new UnsupportedOperationException("Leak detection cannot be combined with --only-ticks-over, because leak correlation needs one continuous recording.");
            }
            // Both the primary Allocation collector and the HeapLeak collector emit 'alloc=' and
            // async-profiler takes last-wins, so combining them silently overrides the user's
            // --interval. Refuse rather than profile at a rate they did not ask for.
            if (this.heapLeaks && this.mode == SamplerMode.ALLOCATION) {
                throw new UnsupportedOperationException("--heap-leaks cannot be combined with --alloc: they both configure allocation profiling and would conflict. Use --heap-leaks on its own - it already reports retained objects.");
            }
            if (this.autoEndTime == -1) {
                throw new UnsupportedOperationException("Leak detection requires a --timeout, because it records every native allocation and window rotation is disabled. An unbounded run can write gigabytes to disk. Try --timeout 900.");
            }
        }

        Sampler sampler;
        if (canUseAsyncProfiler) {
            SampleCollector<?> collector = this.mode == SamplerMode.ALLOCATION
                    ? new SampleCollector.Allocation(interval, this.allocLiveOnly)
                    : new SampleCollector.Execution(interval);
            AsyncSampler asyncSampler = onlyTicksOverMode
                    ? new AsyncSampler(platform, settings, collector, this.ticksOver)
                    : new AsyncSampler(platform, settings, collector);

            // fork - attach the leak engines to the same recording
            if (this.nativeMemoryLeaks) {
                asyncSampler.addExtraCollector(new SampleCollector.NativeMemory(SamplerMode.NATIVE_MEMORY.defaultInterval()));
            }
            if (this.heapLeaks) {
                asyncSampler.addExtraCollector(new SampleCollector.HeapLeak(SamplerMode.ALLOCATION.defaultInterval()));
            }

            sampler = asyncSampler;
        } else {
            sampler = onlyTicksOverMode
                    ? new JavaSampler(platform, settings, this.tickHook, this.ticksOver)
                    : new JavaSampler(platform, settings);
        }

        sampler.start();
        return sampler;
    }

}
