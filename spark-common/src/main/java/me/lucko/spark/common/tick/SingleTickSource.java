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

package me.lucko.spark.common.tick;

/**
 * fork - admits tick notifications from one thread only.
 *
 * <p>On regionised servers (Folia and its forks, e.g. Canvas) there is no main thread. Each
 * region ticks on its own thread and fires the server tick events independently, so a listener
 * registered once receives one notification per region per tick, concurrently, from many
 * threads.</p>
 *
 * <p>That breaks spark's tick statistics twice over. The visible failure is corruption: the TPS
 * rolling averages do read-modify-write on a shared array index, so concurrent callers walk the
 * index past the array length and throw ArrayIndexOutOfBoundsException out of the event handler.
 * The quieter failure is arithmetic: were the race fixed alone, N regions each ticking at 20 TPS
 * would be counted as N x 20, and spark would cheerfully report 100 TPS on a healthy five-region
 * server.</p>
 *
 * <p>Admitting a single thread solves both. The reported figure becomes the tick rate of one
 * region rather than a meaningless sum - which is the honest measurement available here, since a
 * regionised server has no single global tick rate to report. On Paper and Spigot only one thread
 * ever ticks, so this is a no-op and needs no platform detection.</p>
 *
 * <p>The claim is released if the owning thread stops reporting, so a region that is merged away
 * or shut down does not freeze the statistics permanently.</p>
 */
public final class SingleTickSource {

    /** How long the owner may go quiet before another thread may claim it. */
    private static final long STALE_NANOS = 2_000_000_000L;

    private volatile Thread owner;
    private volatile long lastReport;

    /** Returns true if the calling thread is the admitted tick source. */
    public boolean accept() {
        Thread current = Thread.currentThread();
        long now = System.nanoTime();

        Thread owner = this.owner;
        if (owner == current) {
            this.lastReport = now;
            return true;
        }

        if (owner == null || !owner.isAlive() || (now - this.lastReport) > STALE_NANOS) {
            synchronized (this) {
                Thread current0 = this.owner;
                long now0 = System.nanoTime();
                if (current0 == null || current0 == current || !current0.isAlive()
                        || (now0 - this.lastReport) > STALE_NANOS) {
                    this.owner = current;
                    this.lastReport = now0;
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * True once some thread has claimed the source.
     *
     * <p>Kept for callers that want to distinguish "no ticks yet" from "ticking normally" - the
     * two are otherwise indistinguishable from a zero tick count.</p>
     */
    public boolean claimed() {
        return this.owner != null;
    }

    /** The thread currently admitted, or null. Useful when reporting which region is measured. */
    public Thread owner() {
        return this.owner;
    }
}
