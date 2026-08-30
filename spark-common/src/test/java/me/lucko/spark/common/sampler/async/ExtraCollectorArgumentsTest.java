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
import me.lucko.spark.common.sampler.async.AsyncProfilerAccess.ProfilingEvent;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * fork - guards the rule that additional sample collectors contribute additive engine arguments
 * only.
 *
 * <p>async-profiler's {@code event=} is single-valued: a second one replaces the first. An extra
 * collector that emits {@code event=} therefore switches the primary collector off - a
 * {@code --leaks} run would silently become an allocation-only recording with no execution samples
 * in it, while the profile still reports {@code has_execution = true}. Nothing about the result
 * indicates that anything went wrong, which is why this has to fail at start time instead.</p>
 */
class ExtraCollectorArgumentsTest {

    @Test
    void additiveEngineArgumentsAreAccepted() {
        ImmutableList.Builder<String> command = ImmutableList.builder();

        AsyncProfilerJob.addExtraCollectorArguments(command, ImmutableList.of("alloc=524287", "live"));
        AsyncProfilerJob.addExtraCollectorArguments(command, ImmutableList.of("nativemem=0"));

        assertEquals(ImmutableList.of("alloc=524287", "live", "nativemem=0"), command.build());
    }

    @Test
    void anEventArgumentIsRefused() {
        ImmutableList.Builder<String> command = ImmutableList.builder();

        IllegalStateException e = assertThrows(
                IllegalStateException.class,
                () -> AsyncProfilerJob.addExtraCollectorArguments(command, ImmutableList.of("event=alloc", "alloc=524287"))
        );
        assertTrue(e.getMessage().contains("event=alloc"), e.getMessage());
    }

    @Test
    void heapLeakContributesNoEventArgument() {
        // the collector that actually caused this: HeapLeak runs alongside the execution
        // collector, so an 'event=' of its own would take the execution profile with it.
        // Asserted against the collector's real output, not a stand-in string, because the
        // regression this guards against is a change to HeapLeak.initArguments itself.
        AsyncProfilerAccess access = new AsyncProfilerAccess(ProfilingEvent.WALL, ProfilingEvent.ALLOC);
        Collection<String> arguments = new SampleCollector.HeapLeak(524288).initArguments(access);

        assertEquals(ImmutableList.of("alloc=524288", "live"), ImmutableList.copyOf(arguments));

        ImmutableList.Builder<String> command = ImmutableList.builder();
        AsyncProfilerJob.addExtraCollectorArguments(command, arguments);
        assertEquals(ImmutableList.of("alloc=524288", "live"), command.build());
    }

}
