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
import me.lucko.spark.common.command.sender.CommandSender;
import me.lucko.spark.common.monitor.Metrics;
import me.lucko.spark.common.monitor.memory.GarbageCollectorStatistics;
import me.lucko.spark.common.platform.SparkMetadata;
import me.lucko.spark.common.sampler.aggregator.DataAggregator;
import me.lucko.spark.common.sampler.node.ThreadNode;
import me.lucko.spark.common.sampler.node.exporter.NodeExporter;
import me.lucko.spark.common.sampler.source.ClassSourceLookup;
import me.lucko.spark.common.sampler.window.ProtoTimeEncoder;
import me.lucko.spark.common.sampler.window.WindowStatisticsCollector;
import me.lucko.spark.common.util.TimeUtil;
import me.lucko.spark.common.util.classfinder.ClassFinder;
import me.lucko.spark.common.ws.SamplerViewerSocket;
import me.lucko.spark.common.ws.ViewerSocket;
import me.lucko.spark.proto.SparkProtos;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerData;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerMetadata;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Base implementation class for {@link Sampler}s.
 */
public abstract class AbstractSampler implements Sampler {

    /** The spark platform instance */
    protected final SparkPlatform platform;

    /** The interval to wait between sampling, in microseconds */
    protected final int interval;

    /** The instance used to generate thread information for use in sampling */
    protected final ThreadDumper threadDumper;

    /** The time when sampling first began */
    // volatile: written on the thread that starts/stops the sampler, read when the profile is
    // exported - which happens on the export future's thread, and repeatedly on a socket thread
    // for a live viewer. A long is not even read atomically without it.
    protected volatile long startTime = -1;

    /** The time when sampling stopped, or -1 if it is still running */
    protected volatile long endTime = -1;

    /** The unix timestamp (in millis) when this sampler should automatically complete. */
    protected final long autoEndTime; // -1 for nothing

    /** If the sampler is running in the background */
    protected boolean background;

    /** Collects statistics for each window in the sample */
    protected final WindowStatisticsCollector windowStatisticsCollector;

    /** A future to encapsulate the completion of this sampler instance */
    protected final CompletableFuture<Sampler> future = new CompletableFuture<>();

    /** The garbage collector statistics when profiling started */
    protected Map<String, GarbageCollectorStatistics> initialGcStats;

    /** A set of viewer sockets linked to the sampler */
    protected List<SamplerViewerSocket> viewerSockets = new CopyOnWriteArrayList<>();

    protected AbstractSampler(SparkPlatform platform, SamplerSettings settings) {
        this.platform = platform;
        this.interval = settings.interval();
        this.threadDumper = settings.threadDumper();
        this.autoEndTime = settings.autoEndTime();
        this.background = settings.runningInBackground();
        this.windowStatisticsCollector = new WindowStatisticsCollector(platform);
    }

    @Override
    public long getStartTime() {
        if (this.startTime == -1) {
            throw new IllegalStateException("Not yet started");
        }
        return this.startTime;
    }

    @Override
    public long getAutoEndTime() {
        return this.autoEndTime;
    }

    @Override
    public boolean isRunningInBackground() {
        return this.background;
    }

    @Override
    public CompletableFuture<Sampler> getFuture() {
        return this.future;
    }

    protected void recordInitialGcStats() {
        this.initialGcStats = GarbageCollectorStatistics.pollStats();
    }

    protected Map<String, GarbageCollectorStatistics> getInitialGcStats() {
        return this.initialGcStats;
    }

    @Override
    public Map<Integer, SparkProtos.WindowStatistics> exportWindowStatistics() {
        return this.windowStatisticsCollector.export();
    }

    @Override
    public void start() {
        this.startTime = TimeUtil.monotonicCurrentTimeMillis();
    }

    @Override
    public void stop(boolean cancelled) {
        // stop() is reachable twice - the timeout task and an explicit '/spark profiler stop' race
        // for it - and everything below has to happen exactly once. A second endTime would
        // overwrite the real one with a later timestamp, inflating the recording's apparent
        // duration and understating any rate computed from it; and the attached viewers would be
        // told the sampler stopped twice.
        if (this.endTime != -1) {
            return;
        }

        this.endTime = TimeUtil.monotonicCurrentTimeMillis();
        this.windowStatisticsCollector.stop();
        for (SamplerViewerSocket viewerSocket : this.viewerSockets) {
            viewerSocket.processSamplerStopped();
        }
    }

    @Override
    public void attachSocket(SamplerViewerSocket socket) {
        this.viewerSockets.add(socket);
    }

    @Override
    public Collection<SamplerViewerSocket> getAttachedSockets() {
        return this.viewerSockets;
    }

    protected void processWindowRotate() {
        this.viewerSockets.removeIf(socket -> {
            if (!socket.isOpen()) {
                return true;
            }

            socket.processWindowRotate(this);
            return false;
        });
    }

    protected void sendStatisticsToSocket() {
        try {
            this.viewerSockets.removeIf(socket -> !socket.isOpen());
            if (this.viewerSockets.isEmpty()) {
                return;
            }

            SparkProtos.PlatformStatistics platform = this.platform.getStatisticsProvider().getPlatformStatistics(getInitialGcStats(), false);
            SparkProtos.SystemStatistics system = this.platform.getStatisticsProvider().getSystemStatistics();
            SparkProtos.Metrics metrics = Metrics.exportProto();

            for (ViewerSocket viewerSocket : this.viewerSockets) {
                viewerSocket.sendUpdatedStatistics(platform, system, metrics);
            }
        } catch (Exception e) {
            this.platform.getPlugin().log(Level.WARNING, "Exception occurred while sending statistics to viewer", e);
        }
    }

    protected void writeMetadataToProto(SamplerData.Builder proto, SparkPlatform platform, CommandSender.Data creator, String comment, DataAggregator dataAggregator) {
        SamplerMetadata.Builder metadata = SamplerMetadata.newBuilder()
                .setSamplerEngine(getType().asProto())
                .setSamplerMode(getMode().asProto())
                .setStartTime(this.startTime)
                .setInterval(this.interval)
                .setThreadDumper(this.threadDumper.getMetadata())
                .setDataAggregator(dataAggregator.getMetadata());

        SparkMetadata.gather(platform, creator, getInitialGcStats()).writeTo(metadata);

        if (comment != null) {
            metadata.setComment(comment);
        }

        String libraryVersion = getLibraryVersion();
        if (libraryVersion != null) {
            metadata.setSamplerEngineVersion(libraryVersion);
        }

        int totalTicks = this.windowStatisticsCollector.getTotalTicks();
        if (totalTicks != -1) {
            metadata.setNumberOfTicks(totalTicks);
        }

        proto.setMetadata(metadata);
    }

    protected ProtoTimeEncoder writeDataToProto(SamplerData.Builder proto, DataAggregator dataAggregator, Function<ProtoTimeEncoder, NodeExporter> nodeExporterFunction, ClassSourceLookup classSourceLookup, Supplier<ClassFinder> classFinderSupplier) {
        return writeDataToProto(proto, dataAggregator, nodeExporterFunction, classSourceLookup, classFinderSupplier, Collections.emptyList());
    }

    /**
     * Writes the primary data tree, the profile-wide time window list and the class source
     * mappings.
     *
     * @param additionalTimeWindowSources trees that will be written into the same profile later,
     *        and whose time windows must therefore be covered by the profile-wide window list
     * @return the encoder used for the primary tree, so additional trees can be encoded over the
     *         same key set
     */
    protected ProtoTimeEncoder writeDataToProto(SamplerData.Builder proto, DataAggregator dataAggregator, Function<ProtoTimeEncoder, NodeExporter> nodeExporterFunction, ClassSourceLookup classSourceLookup, Supplier<ClassFinder> classFinderSupplier, List<ThreadNode> additionalTimeWindowSources) {
        List<ThreadNode> data = dataAggregator.exportData();
        data.sort(Comparator.comparing(ThreadNode::getThreadLabel));

        ClassSourceLookup.Visitor classSourceVisitor = ClassSourceLookup.createVisitor(classSourceLookup, classFinderSupplier);

        // fork - the encoder's key set has to span every tree written into this profile, not just
        // the primary one. 'time_windows' is profile-wide, so a 'times' array is only meaningful
        // when indexed against it; and an encoder built from the primary tree alone would throw
        // outright for a window only an extra tree recorded.
        List<ThreadNode> timeWindowSources = additionalTimeWindowSources.isEmpty()
                ? data
                : ImmutableList.<ThreadNode>builder().addAll(data).addAll(additionalTimeWindowSources).build();

        ProtoTimeEncoder timeEncoder = new ProtoTimeEncoder(getMode().valueTransformer(), timeWindowSources);
        int[] timeWindows = timeEncoder.getKeys();
        for (int timeWindow : timeWindows) {
            proto.addTimeWindows(timeWindow);
        }

        this.windowStatisticsCollector.ensureHasStatisticsForAllWindows(timeWindows);
        proto.putAllTimeWindowStatistics(this.windowStatisticsCollector.export());

        NodeExporter exporter = nodeExporterFunction.apply(timeEncoder);

        for (ThreadNode entry : data) {
            proto.addThreads(exporter.export(entry));
            classSourceVisitor.visit(entry);
        }

        if (classSourceVisitor.hasClassSourceMappings()) {
            proto.putAllClassSources(classSourceVisitor.getClassSourceMapping());
        }

        if (classSourceVisitor.hasMethodSourceMappings()) {
            proto.putAllMethodSources(classSourceVisitor.getMethodSourceMapping());
        }

        if (classSourceVisitor.hasLineSourceMappings()) {
            proto.putAllLineSources(classSourceVisitor.getLineSourceMapping());
        }

        return timeEncoder;
    }

    /**
     * fork - exports an additional data tree into one of the fork-specific proto fields.
     *
     * <p>Deliberately separate from {@link #writeDataToProto}: that method also writes the
     * time-window list, window statistics and class-source mappings, all of which are
     * profile-wide and must be written exactly once from the primary (execution) tree. Calling
     * it a second time for the native-memory or heap-leak tree would duplicate those and
     * corrupt the profile.</p>
     *
     * <p>The value transformer is fixed to identity here because both extra trees are measured
     * in bytes already - unlike execution time, which is converted from microseconds.</p>
     *
     * @param data the already-exported and sorted tree
     * @param primaryTimeEncoder the encoder used for the primary tree, whose key set is shared
     * @param nodeExporterFunction the node exporter factory
     * @param consumer receives each exported thread node
     * @return total bytes across the exported tree, for the ExtendedProfileContents summary
     */
    protected long writeExtraDataToProto(List<ThreadNode> data, ProtoTimeEncoder primaryTimeEncoder, Function<ProtoTimeEncoder, NodeExporter> nodeExporterFunction, Consumer<me.lucko.spark.proto.SparkSamplerProtos.ThreadNode> consumer) {
        if (data.isEmpty()) {
            return 0L;
        }

        // The key set is shared with the primary tree so the encoded arrays line up with the
        // profile-wide time window list. The value transformer is not: these trees are measured
        // in bytes already, unlike execution time which is converted from microseconds.
        ProtoTimeEncoder timeEncoder = ProtoTimeEncoder.withSameKeys(value -> value, primaryTimeEncoder);
        NodeExporter exporter = nodeExporterFunction.apply(timeEncoder);

        long total = 0L;
        for (ThreadNode entry : data) {
            me.lucko.spark.proto.SparkSamplerProtos.ThreadNode exported = exporter.export(entry);
            consumer.accept(exported);
            for (double t : exported.getTimesList()) {
                total += (long) t;
            }
        }
        return total;
    }
}
