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

import me.lucko.spark.common.sampler.async.AsyncProfilerJob.AddressState;
import me.lucko.spark.common.sampler.async.jfr.JfrReader;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * fork - guards the malloc/free net-counting that native memory leak detection is built on.
 *
 * <p>The property under test is memory, not correctness of the leak list: the tracking map is
 * the one structure in the fork that grows with the workload, and it is only safe to run inside a
 * game server because a settled address is evicted the moment its balance returns to zero. If
 * settled entries are retained, the map stops being bounded by outstanding memory and starts
 * growing with total allocation history - millions of entries on a busy server, allocated inside
 * the server at {@code /spark profiler stop}. Nothing about the resulting profile looks wrong;
 * the server just runs out of heap.</p>
 */
class NativeMemoryLeakTrackingTest {

    private static JfrReader.MallocEvent malloc(long address, long size) {
        return new JfrReader.MallocEvent(1000L, 7, 3, address, size);
    }

    private static JfrReader.MallocEvent free(long address) {
        // async-profiler reports a free as a malloc event with size == 0
        return new JfrReader.MallocEvent(2000L, 7, 3, address, 0);
    }

    @Test
    void settledAllocationsAreEvicted() {
        Map<Long, AddressState> states = new HashMap<>();

        for (long address = 1; address <= 10_000; address++) {
            assertTrue(AsyncProfilerJob.track(states, malloc(address, 128)));
            assertTrue(AsyncProfilerJob.track(states, free(address)));
        }

        assertEquals(0, states.size(), "settled malloc/free pairs must not be retained");
    }

    @Test
    void outstandingAllocationsAreRetained() {
        Map<Long, AddressState> states = new HashMap<>();

        AsyncProfilerJob.track(states, malloc(1, 128));
        AsyncProfilerJob.track(states, malloc(2, 256));
        AsyncProfilerJob.track(states, free(1));

        assertEquals(1, states.size());

        AddressState leaked = states.get(2L);
        assertEquals(1, leaked.balance);
        assertEquals(256, leaked.size);
        assertEquals(7, leaked.tid);
        assertEquals(3, leaked.stackTraceId);
    }

    @Test
    void addressReuseRetainsTheOutstandingAllocation() {
        Map<Long, AddressState> states = new HashMap<>();

        AsyncProfilerJob.track(states, malloc(1, 128));
        AsyncProfilerJob.track(states, free(1));
        AsyncProfilerJob.track(states, malloc(1, 4096));

        assertEquals(1, states.size());

        AddressState state = states.get(1L);
        assertEquals(1, state.balance);
        assertEquals(4096, state.size, "the details of the allocation still outstanding must win");
    }

    @Test
    void freeSeenBeforeItsMallocCancelsOut() {
        // async-profiler buffers per thread and flushes at dump, so a free is routinely read
        // before the malloc it belongs to. It must net out, not register as a phantom leak.
        Map<Long, AddressState> states = new HashMap<>();

        AsyncProfilerJob.track(states, free(1));
        assertEquals(-1, states.get(1L).balance);

        AsyncProfilerJob.track(states, malloc(1, 128));
        assertEquals(0, states.size());
    }

    @Test
    void freeWithoutAMallocIsNotReportedAsALeak() {
        // normal for anything allocated before profiling began
        Map<Long, AddressState> states = new HashMap<>();

        AsyncProfilerJob.track(states, free(1));

        AddressState state = states.get(1L);
        assertEquals(-1, state.balance);
        assertEquals(0L, state.time, "no malloc was seen, so there are no allocation details");
    }

    @Test
    void trackingIsOrderIndependent() {
        Map<Long, AddressState> forwards = new HashMap<>();
        AsyncProfilerJob.track(forwards, malloc(1, 128));
        AsyncProfilerJob.track(forwards, malloc(1, 128));
        AsyncProfilerJob.track(forwards, free(1));

        Map<Long, AddressState> backwards = new HashMap<>();
        AsyncProfilerJob.track(backwards, free(1));
        AsyncProfilerJob.track(backwards, malloc(1, 128));
        AsyncProfilerJob.track(backwards, malloc(1, 128));

        assertEquals(forwards.get(1L).balance, backwards.get(1L).balance);
    }

    @Test
    void trackReportsWhenAnAddressCannotBeTracked() {
        // the cap itself is 2,000,000, so what is exercised here is the reporting contract rather
        // than the value: one event, accepted by a map with room and refused by one without
        JfrReader.MallocEvent event = malloc(1, 128);

        Map<Long, AddressState> withRoom = new HashMap<>();
        assertTrue(AsyncProfilerJob.track(withRoom, event));

        Map<Long, AddressState> full = new HashMap<Long, AddressState>() {
            @Override
            public int size() {
                return Integer.MAX_VALUE;
            }
        };
        assertFalse(AsyncProfilerJob.track(full, event));
        assertNull(full.get(1L), "an address that could not be tracked must not be recorded");
    }

}
