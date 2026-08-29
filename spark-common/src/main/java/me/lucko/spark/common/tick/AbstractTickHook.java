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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public abstract class AbstractTickHook implements TickHook {

    private final Set<Callback> tasks = new CopyOnWriteArraySet<>();
    // fork - AtomicInteger, and gated on a single reporting thread: on a regionised server every
    // region fires the tick event, so both the counter increment and the downstream statistics
    // would otherwise be racing, and the tick count would advance once per region per tick.
    private final java.util.concurrent.atomic.AtomicInteger tick = new java.util.concurrent.atomic.AtomicInteger();
    private final SingleTickSource source = new SingleTickSource();

    protected void onTick() {
        if (!this.source.accept()) {
            return;
        }
        int current = this.tick.getAndIncrement();
        for (Callback r : this.tasks) {
            r.onTick(current);
        }
    }

    @Override
    public int getCurrentTick() {
        return this.tick.get();
    }

    @Override
    public void addCallback(Callback runnable) {
        this.tasks.add(runnable);
    }

    @Override
    public void removeCallback(Callback runnable) {
        this.tasks.remove(runnable);
    }

}
