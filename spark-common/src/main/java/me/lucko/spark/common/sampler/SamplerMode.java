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

import me.lucko.spark.proto.SparkSamplerProtos.SamplerMetadata;

import java.util.function.LongToDoubleFunction;

public enum SamplerMode {

    EXECUTION(
            value -> {
                // convert the duration from microseconds -> milliseconds
                return value / 1000d;
            },
            4, // ms
            SamplerMetadata.SamplerMode.EXECUTION
    ),

    ALLOCATION(
            value -> {
                // do nothing
                return value;
            },
            524287, // 512 KiB
            SamplerMetadata.SamplerMode.ALLOCATION
    ),

    // fork - native (off-heap) memory leak profiling.
    // The value is bytes of *unfreed* native memory, so like ALLOCATION it passes through
    // untransformed. Default interval is 0, meaning every malloc is recorded: leaks are
    // frequently many small allocations rather than a few big ones, and a sampling interval
    // would silently drop exactly that shape (async-profiler's own tests show ~2MB
    // allocations vanishing entirely at a 10MB interval).
    NATIVE_MEMORY(
            value -> {
                // do nothing - already bytes
                return value;
            },
            0, // record every malloc
            SamplerMetadata.SamplerMode.NATIVE_MEMORY
    );

    private final LongToDoubleFunction valueTransformer;
    private final int defaultInterval;
    private final SamplerMetadata.SamplerMode proto;

    SamplerMode(LongToDoubleFunction valueTransformer, int defaultInterval, SamplerMetadata.SamplerMode proto) {
        this.valueTransformer = valueTransformer;
        this.defaultInterval = defaultInterval;
        this.proto = proto;
    }

    public LongToDoubleFunction valueTransformer() {
        return this.valueTransformer;
    }

    public int defaultInterval() {
        return this.defaultInterval;
    }

    /**
     * Gets the metadata enum instance for this sampler mode.
     *
     * @return proto metadata
     */
    public SamplerMetadata.SamplerMode asProto() {
        return this.proto;
    }

}
