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
import me.lucko.spark.common.sampler.SamplerMode;
import me.lucko.spark.common.sampler.async.AsyncProfilerAccess.ProfilingEvent;
import me.lucko.spark.common.sampler.async.jfr.JfrReader.AllocationSample;
import me.lucko.spark.common.sampler.async.jfr.JfrReader.Event;
import me.lucko.spark.common.sampler.async.jfr.JfrReader.ExecutionSample;
import me.lucko.spark.common.sampler.async.jfr.JfrReader; // fork

import java.util.Collection;
import java.util.Objects;

/**
 * Collects and processes sample events for a given type.
 *
 * @param <E> the event type
 */
public interface SampleCollector<E extends Event> {

    /**
     * Gets the arguments to initialise the profiler.
     *
     * @param access the async profiler access object
     * @return the initialisation arguments
     */
    Collection<String> initArguments(AsyncProfilerAccess access);

    /**
     * Gets the event class processed by this sample collector.
     *
     * @return the event class
     */
    Class<E> eventClass();

    /**
     * Gets the measurements for a given event
     *
     * @param event the event
     * @return the measurement
     */
    long measure(E event);

    /**
     * Gets the mode for the collector.
     *
     * @return the mode
     */
    SamplerMode getMode();

    /**
     * Sample collector for execution (cpu time) profiles.
     */
    final class Execution implements SampleCollector<ExecutionSample> {
        private final int interval; // time in microseconds

        public Execution(int interval) {
            this.interval = interval;
        }

        @Override
        public Collection<String> initArguments(AsyncProfilerAccess access) {
            ProfilingEvent event = access.getProfilingEvent();
            Objects.requireNonNull(event, "event");

            return ImmutableList.of(
                    "event=" + event,
                    "interval=" + this.interval + "us"
            );
        }

        @Override
        public Class<ExecutionSample> eventClass() {
            return ExecutionSample.class;
        }

        @Override
        public long measure(ExecutionSample event) {
            return event.value() * this.interval;
        }

        @Override
        public SamplerMode getMode() {
            return SamplerMode.EXECUTION;
        }
    }

    /**
     * Sample collector for allocation (memory) profiles.
     */
    final class Allocation implements SampleCollector<AllocationSample> {
        private final int intervalBytes;
        private final boolean liveOnly;

        public Allocation(int intervalBytes, boolean liveOnly) {
            this.intervalBytes = intervalBytes;
            this.liveOnly = liveOnly;
        }

        public boolean isLiveOnly() {
            return this.liveOnly;
        }

        @Override
        public Collection<String> initArguments(AsyncProfilerAccess access) {
            ProfilingEvent event = access.getAllocationProfilingEvent();
            Objects.requireNonNull(event, "event");

            ImmutableList.Builder<String> builder = ImmutableList.builder();
            builder.add("event=" + event);
            builder.add("alloc=" + this.intervalBytes);
            if (this.liveOnly) {
                builder.add("live");
            }
            return builder.build();
        }

        @Override
        public Class<AllocationSample> eventClass() {
            return AllocationSample.class;
        }

        @Override
        public long measure(AllocationSample event) {
            return event.value();
        }

        @Override
        public SamplerMode getMode() {
            return SamplerMode.ALLOCATION;
        }
    }

    /**
     * Sample collector for Java HEAP leaks - objects that survived garbage collection. // fork
     *
     * <p>This exists as its own collector because {@link Allocation} is the wrong data source
     * for a leak question, in a way that is easy to get wrong and expensive to notice.
     * async-profiler's {@code live} flag does NOT filter the {@code AllocationSample} stream to
     * surviving objects; it emits the retained set as a SEPARATE {@code profiler.LiveObject}
     * stream alongside the full allocation profile. Reading {@code AllocationSample} therefore
     * gives you total allocation volume, most of which is collected immediately and perfectly
     * healthy.</p>
     *
     * <p>Measured on a real capture: 58.6 GB of AllocationSample against 605 KB of LiveObject -
     * a 96,805x overstatement. Reporting that as "leaked" would flag every healthy server as
     * catastrophically broken.</p>
     */
    final class HeapLeak implements SampleCollector<JfrReader.LiveObject> {
        private final int intervalBytes;

        public HeapLeak(int intervalBytes) {
            this.intervalBytes = intervalBytes;
        }

        @Override
        public Collection<String> initArguments(AsyncProfilerAccess access) {
            ProfilingEvent event = access.getAllocationProfilingEvent();
            Objects.requireNonNull(event, "event");

            // 'live' is what makes async-profiler emit the LiveObject stream at all.
            return ImmutableList.of(
                    "event=" + event,
                    "alloc=" + this.intervalBytes,
                    "live"
            );
        }

        @Override
        public Class<JfrReader.LiveObject> eventClass() {
            return JfrReader.LiveObject.class;
        }

        @Override
        public long measure(JfrReader.LiveObject event) {
            return event.allocationSize;
        }

        @Override
        public SamplerMode getMode() {
            return SamplerMode.ALLOCATION;
        }
    }

    /**
     * Sample collector for native (off-heap) memory leaks. // fork
     *
     * <p>This measures memory that never appears in the Java heap at all: zlib streams behind
     * Inflater/Deflater, Netty's pooled direct arenas, image codec state, JNI library
     * allocations. The symptom it exists to explain is "RSS climbs for days while the heap
     * stays flat" - which no existing spark mode can see, because spark profiles CPU time and
     * Java allocation, not malloc.</p>
     *
     * <p>Unlike every other collector here, a single event is not meaningful on its own. The
     * profiler emits a {@code profiler.Malloc} event per allocation and a {@code profiler.Free}
     * event (delivered as a MallocEvent with {@code size == 0}) per release; a leak is an
     * allocation with no matching free. Correlating those is a whole-stream operation, so the
     * actual aggregation lives in {@link AsyncProfilerJob}, not in {@link #measure}.</p>
     */
    final class NativeMemory implements SampleCollector<JfrReader.MallocEvent> {
        /** Sampling interval in bytes. 0 records every single malloc. */
        private final int intervalBytes;

        public NativeMemory(int intervalBytes) {
            this.intervalBytes = intervalBytes;
        }

        @Override
        public Collection<String> initArguments(AsyncProfilerAccess access) {
            // 'nativemem' is a standalone argument, not an 'event=' value, which is precisely
            // why it can be combined with event=wall and alloc= in the same recording.
            //
            // Deliberately NOT passing 'nofree': that suppresses free events, which would make
            // every allocation look permanently leaked and render the whole mode useless.
            return ImmutableList.of("nativemem=" + this.intervalBytes);
        }

        @Override
        public Class<JfrReader.MallocEvent> eventClass() {
            return JfrReader.MallocEvent.class;
        }

        @Override
        public long measure(JfrReader.MallocEvent event) {
            return event.size;
        }

        @Override
        public SamplerMode getMode() {
            return SamplerMode.NATIVE_MEMORY;
        }
    }

}
