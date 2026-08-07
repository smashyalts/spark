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

package me.lucko.spark.geyser;

import me.lucko.spark.common.SparkBuildInfo;
import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.SparkPlugin;
import me.lucko.spark.common.command.Command;
import me.lucko.spark.common.monitor.ping.PlayerPingProvider;
import me.lucko.spark.common.platform.PlatformInfo;
import me.lucko.spark.common.sampler.source.ClassSourceLookup;
import me.lucko.spark.common.sampler.source.SourceMetadata;

import org.geysermc.event.subscribe.Subscribe;
import org.geysermc.geyser.api.GeyserApi;
import org.geysermc.geyser.api.command.CommandSource;
import org.geysermc.geyser.api.event.lifecycle.GeyserDefineCommandsEvent;
import org.geysermc.geyser.api.event.lifecycle.GeyserPreInitializeEvent;
import org.geysermc.geyser.api.event.lifecycle.GeyserRegisterPermissionsEvent;
import org.geysermc.geyser.api.event.lifecycle.GeyserShutdownEvent;
import org.geysermc.geyser.api.extension.Extension;
import org.geysermc.geyser.api.util.TriState;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.stream.Stream;

/**
 * spark for Geyser, implemented as a Geyser extension.
 *
 * <p>Works on Geyser Standalone as well as Geyser running as a plugin/mod on
 * another platform. On Standalone this is the only way to profile the Geyser
 * process from the inside.</p>
 *
 * <p>Geyser has no tick loop, so no {@link me.lucko.spark.common.tick.TickHook}
 * or {@link me.lucko.spark.common.tick.TickReporter} is provided and the
 * TPS/MSPT commands are unavailable — the same as on the Velocity and
 * BungeeCord proxy modules. Everything else (the profiler, including the
 * {@code --leaks} native/heap leak modes, {@code /spark health},
 * {@code /spark gc}, {@code /spark heapsummary}) works normally.</p>
 */
public class GeyserSparkPlugin implements Extension, SparkPlugin {

    private SparkPlatform platform;
    private ExecutorService asyncExecutor;

    @Subscribe
    public void onPreInitialize(GeyserPreInitializeEvent event) {
        // The platform has to be enabled before GeyserDefineCommandsEvent fires,
        // because command registration below enumerates spark's own command list.
        AtomicInteger threadCount = new AtomicInteger();
        this.asyncExecutor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "spark-geyser-async-worker-" + threadCount.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });

        this.platform = new SparkPlatform(this);
        this.platform.enable();
    }

    @Subscribe
    public void onRegisterPermissions(GeyserRegisterPermissionsEvent event) {
        // Geyser Standalone keeps its own permissions file, so spark's permission
        // nodes have to be declared to it or they can never be granted there.
        // Default to FALSE: profiling is an admin action, console is always allowed.
        for (String permission : this.platform.getCommandManager().getAllSparkPermissions()) {
            event.register(permission, TriState.FALSE);
        }
    }

    @Subscribe
    public void onDefineCommands(GeyserDefineCommandsEvent event) {
        // Geyser namespaces extension commands under the extension's root command
        // (which defaults to the extension id, "spark"). Registering each of spark's
        // own top-level commands here gives the familiar "/spark profiler start ..."
        // rather than a single catch-all subcommand.
        for (Command command : this.platform.getCommandManager().getCommands()) {
            String primaryAlias = command.primaryAlias();
            List<String> aliases = new ArrayList<>(command.aliases());
            aliases.remove(primaryAlias);

            event.register(org.geysermc.geyser.api.command.Command.builder(this)
                    .source(CommandSource.class)
                    .name(primaryAlias)
                    .aliases(aliases)
                    .description("spark " + primaryAlias)
                    .permission("spark." + primaryAlias)
                    .executor((source, cmd, args) -> {
                        // spark expects the subcommand name as args[0]; Geyser has
                        // already consumed it as the command name, so put it back.
                        String[] sparkArgs = new String[args.length + 1];
                        sparkArgs[0] = primaryAlias;
                        System.arraycopy(args, 0, sparkArgs, 1, args.length);
                        this.platform.executeCommand(new GeyserSparkCommandSender(source), sparkArgs);
                    })
                    .build()
            );
        }
    }

    @Subscribe
    public void onShutdown(GeyserShutdownEvent event) {
        if (this.platform != null) {
            this.platform.disable();
            this.platform = null;
        }
        if (this.asyncExecutor != null) {
            this.asyncExecutor.shutdown();
            try {
                if (!this.asyncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    this.asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            this.asyncExecutor = null;
        }
    }

    @Override
    public String getVersion() {
        return SparkBuildInfo.VERSION;
    }

    @Override
    public Path getPluginDirectory() {
        return dataFolder();
    }

    @Override
    public String getCommandName() {
        // Used only to render usage strings. Geyser routes extension commands
        // through the extension's root command, which is the extension id.
        return rootCommand();
    }

    @Override
    public Stream<GeyserSparkCommandSender> getCommandSenders() {
        return Stream.concat(
                GeyserApi.api().onlineConnections().stream(),
                Stream.of(GeyserApi.api().consoleCommandSource())
        ).map(GeyserSparkCommandSender::new);
    }

    @Override
    public void executeAsync(Runnable task) {
        this.asyncExecutor.execute(task);
    }

    @Override
    public void log(Level level, String msg) {
        if (level.intValue() >= 1000) { // severe
            logger().error(msg);
        } else if (level.intValue() >= 900) { // warning
            logger().warning(msg);
        } else {
            logger().info(msg);
        }
    }

    @Override
    public void log(Level level, String msg, Throwable throwable) {
        if (level.intValue() >= 1000) { // severe
            logger().error(msg, throwable);
        } else if (level.intValue() >= 900) { // warning
            logger().warning(msg + ": " + throwable);
        } else {
            logger().info(msg + ": " + throwable);
        }
    }

    @Override
    public ClassSourceLookup createClassSourceLookup() {
        return new GeyserClassSourceLookup();
    }

    @Override
    public Collection<SourceMetadata> getKnownSources() {
        return SourceMetadata.gather(
                GeyserApi.api().extensionManager().extensions(),
                extension -> extension.description().id(),
                extension -> extension.description().version(),
                extension -> String.join(", ", extension.description().authors()),
                extension -> null
        );
    }

    @Override
    public PlayerPingProvider createPlayerPingProvider() {
        return new GeyserPlayerPingProvider();
    }

    @Override
    public PlatformInfo getPlatformInfo() {
        return new GeyserPlatformInfo();
    }
}
