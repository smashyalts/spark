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

package me.lucko.spark.common.monitor.cpu;

import org.junit.jupiter.api.Test;

import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class CpuMonitorTest {

    /**
     * Both readings are documented to return a negative value when they are unavailable, which
     * happens for two quite different reasons:
     *
     * <ul>
     *     <li>temporarily, in a freshly started JVM, until the bean has two samples to compare</li>
     *     <li>permanently, on hosts where the OS cannot supply the figure at all (observed on
     *         Windows machines whose performance counters are unavailable, where the underlying
     *         {@code SystemCpuLoad} and {@code ProcessCpuLoad} attributes read -1.0 forever)</li>
     * </ul>
     *
     * <p>The first is worth waiting out; the second is a property of the machine, not of spark, so
     * asserting through it would just make the suite fail on those hosts.</p>
     */
    @Test
    public void testCpuLoad() throws InterruptedException {
        double processLoad = awaitReading(CpuMonitor::processLoad);
        double systemLoad = awaitReading(CpuMonitor::systemLoad);

        assumeTrue(processLoad >= 0 && systemLoad >= 0, "cpu load reporting is not available on this system");

        assertTrue(processLoad <= 1, "process cpu load out of range: " + processLoad);
        assertTrue(systemLoad <= 1, "system cpu load out of range: " + systemLoad);
    }

    private static double awaitReading(DoubleSupplier supplier) throws InterruptedException {
        double value = supplier.getAsDouble();
        for (int i = 0; i < 20 && value < 0; i++) {
            Thread.sleep(100);
            value = supplier.getAsDouble();
        }
        return value;
    }

}
