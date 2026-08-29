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

package me.lucko.spark.bukkit;

import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;

/**
 * fork - scheduling that works on both Bukkit-family servers and Folia.
 *
 * <p>Folia removes {@link org.bukkit.scheduler.BukkitScheduler} entirely: there is no single main
 * thread to schedule onto, only independently ticking regions. Calls to
 * {@code Bukkit.getScheduler()} throw {@link UnsupportedOperationException} there, so the three
 * places spark schedules work need a fork in the road.</p>
 *
 * <p>The two implementations live in separate classes on purpose. Class verification resolves the
 * types named in a class's methods when that class is first loaded, so if the Folia calls sat in
 * a branch of a shared class, merely loading it on Spigot would raise NoClassDefFoundError for
 * the Folia scheduler types - the branch never running is not enough. Keeping them apart means
 * {@link FoliaImpl} is only ever loaded on a server that actually has those classes.</p>
 */
public interface SparkScheduler {

    /** A handle to a repeating task, cancellable on either platform. */
    interface Task {
        void cancel();
    }

    void executeAsync(Runnable task);

    /**
     * Runs a task on the server's main execution context.
     *
     * <p>On Folia this is the global region, which owns server-wide state rather than any
     * particular world region. That is the correct target for spark's purposes - reporting
     * command output and reading server-wide statistics - but it is not a general replacement
     * for the main thread, and anything touching entities or blocks would need a region or
     * entity scheduler instead.</p>
     */
    void executeSync(Runnable task);

    /** Schedules a task every {@code periodTicks}, starting after {@code delayTicks}. */
    Task scheduleRepeating(Runnable task, long delayTicks, long periodTicks);

    /** Cancels everything this plugin has scheduled. Called on disable. */
    void cancelAll();

    static boolean isFolia() {
        try {
            Class.forName("io.papermc.paper.threadedregions.RegionizedServer");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    static SparkScheduler create(Plugin plugin) {
        return isFolia() ? new FoliaImpl(plugin) : new BukkitImpl(plugin);
    }

    /** Standard Bukkit/Spigot/Paper scheduling. */
    final class BukkitImpl implements SparkScheduler {
        private final Plugin plugin;

        BukkitImpl(Plugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public void executeAsync(Runnable task) {
            this.plugin.getServer().getScheduler().runTaskAsynchronously(this.plugin, task);
        }

        @Override
        public void executeSync(Runnable task) {
            this.plugin.getServer().getScheduler().runTask(this.plugin, task);
        }

        @Override
        public Task scheduleRepeating(Runnable task, long delayTicks, long periodTicks) {
            org.bukkit.scheduler.BukkitTask handle =
                    this.plugin.getServer().getScheduler().runTaskTimer(this.plugin, task, delayTicks, periodTicks);
            return handle::cancel;
        }

        @Override
        public void cancelAll() {
            this.plugin.getServer().getScheduler().cancelTasks(this.plugin);
        }
    }

    /**
     * Folia scheduling.
     *
     * <p>Never loaded on a non-Folia server - see the interface javadoc for why that separation
     * is load-bearing rather than stylistic.</p>
     */
    final class FoliaImpl implements SparkScheduler {
        private final Plugin plugin;

        FoliaImpl(Plugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public void executeAsync(Runnable task) {
            Bukkit.getAsyncScheduler().runNow(this.plugin, scheduled -> task.run());
        }

        @Override
        public void executeSync(Runnable task) {
            Bukkit.getGlobalRegionScheduler().execute(this.plugin, task);
        }

        @Override
        public Task scheduleRepeating(Runnable task, long delayTicks, long periodTicks) {
            // Folia rejects a delay or period below one tick, where Bukkit tolerates zero.
            long delay = Math.max(1, delayTicks);
            long period = Math.max(1, periodTicks);

            io.papermc.paper.threadedregions.scheduler.ScheduledTask handle =
                    Bukkit.getGlobalRegionScheduler()
                            .runAtFixedRate(this.plugin, scheduled -> task.run(), delay, period);
            return handle::cancel;
        }

        @Override
        public void cancelAll() {
            Bukkit.getAsyncScheduler().cancelTasks(this.plugin);
            Bukkit.getGlobalRegionScheduler().cancelTasks(this.plugin);
        }
    }
}
