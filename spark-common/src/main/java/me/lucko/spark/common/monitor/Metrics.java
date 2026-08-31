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

package me.lucko.spark.common.monitor;

import me.lucko.spark.common.sampler.window.WindowStatisticsCollector;
import me.lucko.spark.common.util.MetricSeries;
import me.lucko.spark.common.util.TimeUtil;
import me.lucko.spark.proto.SparkProtos;

import java.time.Duration;

/**
 * A collection of metrics series used for monitoring the server.
 *
 * <p>The metric series have a fixed retention period and an interval between recordings.
 * The retention period determines how long the recorded metrics are kept,
 * while the interval determines how often new metrics are recorded.</p>
 *
 * <p>These metrics are recorded at a higher interval than those collected by {@link WindowStatisticsCollector}.</p>
 */
public enum Metrics {
    ;

    /** The retention period of the metrics series. */
    private static final Duration RETENTION = Duration.ofHours(1);

    /** The interval between metric recordings. */
    public static final int INTERVAL_MILLIS = (int) Duration.ofSeconds(10).toMillis();

    /** The estimated capacity of the metrics series. */
    private static final int INITIAL_CAPACITY = Math.toIntExact(RETENTION.toMillis() / INTERVAL_MILLIS) + 1;

    /**
     * The timestamp after which metrics recording should start.
     *
     * <p>A delay avoids recording incomplete values when the server first starts</p>
     */
    private static final long START_RECORDING_MILLIS = TimeUtil.monotonicCurrentTimeMillis() + INTERVAL_MILLIS;

    public static final MetricSeries.Doubles TPS = new MetricSeries.Doubles(RETENTION, INITIAL_CAPACITY);
    public static final MetricSeries.Averages TICK_DURATION = new MetricSeries.Averages(RETENTION, INITIAL_CAPACITY);
    public static final MetricSeries.Doubles CPU_USAGE_PROCESS = new MetricSeries.Doubles(RETENTION, INITIAL_CAPACITY);
    public static final MetricSeries.Doubles CPU_USAGE_SYSTEM = new MetricSeries.Doubles(RETENTION, INITIAL_CAPACITY);
    public static final MetricSeries.MemoryUsages MEMORY_USAGE_HEAP = new MetricSeries.MemoryUsages(RETENTION, INITIAL_CAPACITY);
    public static final MetricSeries.MemoryUsages MEMORY_USAGE_NON_HEAP = new MetricSeries.MemoryUsages(RETENTION, INITIAL_CAPACITY);
    public static final MetricSeries.Doubles MEMORY_ALLOCATION = new MetricSeries.Doubles(RETENTION, INITIAL_CAPACITY);
    public static final MetricSeries.WorldInfo WORLD_INFO = new MetricSeries.WorldInfo(RETENTION, INITIAL_CAPACITY);
    public static final MetricSeries.Averages PLAYER_PING = new MetricSeries.Averages(RETENTION, INITIAL_CAPACITY);

    public static boolean shouldRecordTps() {
        return shouldRecord(TPS, TimeUtil.monotonicCurrentTimeMillis());
    }

    public static boolean shouldRecordTickDuration() {
        return shouldRecord(TICK_DURATION, TimeUtil.monotonicCurrentTimeMillis());
    }

    public static boolean shouldRecordCpuUsageProcess(long timeNow) {
        return shouldRecord(CPU_USAGE_PROCESS, timeNow);
    }

    public static boolean shouldRecordCpuUsageSystem(long timeNow) {
        return shouldRecord(CPU_USAGE_SYSTEM, timeNow);
    }

    private static boolean shouldRecord(MetricSeries<?> series, long timeNow) {
        if (timeNow < START_RECORDING_MILLIS) {
            return false;
        }

        long newestTimestamp = series.newestTimestamp();
        return newestTimestamp == 0 || newestTimestamp < timeNow - INTERVAL_MILLIS;
    }

    /**
     * Exports every series that holds samples.
     *
     * <p>Emptiness is decided by {@code toProto} returning null rather than by a separate
     * {@code isEmpty} test: the two were distinct lock acquisitions, so a concurrent recording -
     * this runs on the viewer statistics tick while the monitors record - could prune a series
     * between the guard and the export, and the profile then carried a series claiming to start
     * at the epoch with no values in it.</p>
     */
    public static SparkProtos.Metrics exportProto() {
        SparkProtos.Metrics.Builder builder = SparkProtos.Metrics.newBuilder();

        SparkProtos.DoubleMetricSeries tps = TPS.toProto();
        if (tps != null) builder.setTps(tps);

        SparkProtos.AveragesMetricSeries tickDuration = TICK_DURATION.toProto();
        if (tickDuration != null) builder.setTickDuration(tickDuration);

        SparkProtos.DoubleMetricSeries cpuProcess = CPU_USAGE_PROCESS.toProto();
        if (cpuProcess != null) builder.setCpuUsageProcess(cpuProcess);

        SparkProtos.DoubleMetricSeries cpuSystem = CPU_USAGE_SYSTEM.toProto();
        if (cpuSystem != null) builder.setCpuUsageSystem(cpuSystem);

        SparkProtos.MemoryUsageMetricSeries heap = MEMORY_USAGE_HEAP.toProto();
        if (heap != null) builder.setMemoryUsageHeap(heap);

        SparkProtos.MemoryUsageMetricSeries nonHeap = MEMORY_USAGE_NON_HEAP.toProto();
        if (nonHeap != null) builder.setMemoryUsageNonHeap(nonHeap);

        SparkProtos.DoubleMetricSeries allocation = MEMORY_ALLOCATION.toProto();
        if (allocation != null) builder.setMemoryAllocation(allocation);

        SparkProtos.WorldInfoMetricSeries worldInfo = WORLD_INFO.toProto();
        if (worldInfo != null) builder.setWorldInfo(worldInfo);

        SparkProtos.AveragesMetricSeries ping = PLAYER_PING.toProto();
        if (ping != null) builder.setPlayerPing(ping);

        return builder.build();
    }

}
