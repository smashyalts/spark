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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * fork - guards the collector classification used to route heap leak data into the profile.
 *
 * <p>This exists because the failure it protects against is invisible at runtime: if the
 * classification misses {@link SampleCollector.HeapLeak}, a {@code --leaks} profile still
 * uploads, still renders, and still reports native memory correctly - it simply carries
 * {@code has_heap_leak = false}, which every reader interprets as "heap leak detection was not
 * enabled" rather than "it was enabled and the result was thrown away".</p>
 */
class HeapLeakCollectorClassificationTest {

    @Test
    void heapLeakCollectorIsClassifiedAsHeapLeak() {
        assertTrue(AsyncSampler.isHeapLeakCollector(new SampleCollector.HeapLeak(524288)));
    }

    @Test
    void liveOnlyAllocationCollectorIsClassifiedAsHeapLeak() {
        assertTrue(AsyncSampler.isHeapLeakCollector(new SampleCollector.Allocation(524288, true)));
    }

    @Test
    void plainAllocationCollectorIsNotClassifiedAsHeapLeak() {
        // --alloc without --alloc-live-only is allocation RATE, not retention. Reporting it as
        // leaked heap would flag healthy code as broken.
        assertFalse(AsyncSampler.isHeapLeakCollector(new SampleCollector.Allocation(524288, false)));
    }

    @Test
    void otherCollectorsAreNotClassifiedAsHeapLeak() {
        assertFalse(AsyncSampler.isHeapLeakCollector(new SampleCollector.Execution(4000)));
        assertFalse(AsyncSampler.isHeapLeakCollector(new SampleCollector.NativeMemory(0)));
    }

    /**
     * The specific mistake this file exists to prevent: {@link SampleCollector.HeapLeak} is a
     * sibling of {@link SampleCollector.Allocation}, not a subclass, so an
     * {@code instanceof Allocation} test silently never matches it.
     */
    @Test
    void heapLeakIsNotAnAllocationCollector() {
        assertFalse(SampleCollector.Allocation.class.isAssignableFrom(SampleCollector.HeapLeak.class));
    }
}
