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

package me.lucko.spark.common.monitor.memory;

/**
 * fork - reads and sets Netty's resource leak detection level at runtime.
 *
 * <p>Netty's detector is normally configured with
 * {@code -Dio.netty.leakDetection.level=advanced} at startup, which on a game server means
 * editing a start script and restarting - and the restart destroys the very state being
 * investigated. The level is a plain static that Netty reads on each allocation, so it can be
 * raised on a running server instead.</p>
 *
 * <p>What it does and does not cover is worth being precise about, because the name suggests more
 * than it delivers. This finds reference-counted {@code ByteBuf} instances that became garbage
 * without being released - a Netty-specific programming error, and the usual cause of a Minecraft
 * server slowly losing direct memory to its network stack. It says nothing about malloc-level
 * leaks, mapped files, or arena retention, which is what {@code --investigate} is for.</p>
 *
 * <p>Reflective for the same reason the Netty accounting in {@link ProcessMemorySnapshot} is:
 * spark does not depend on Netty, and it is absent entirely on some platforms.</p>
 */
public enum NettyLeakDetector {
    ;

    private static final String DETECTOR_CLASS = "io.netty.util.ResourceLeakDetector";
    private static final String LEVEL_CLASS = "io.netty.util.ResourceLeakDetector$Level";

    /**
     * The level Netty is currently sampling at, or null if the detector is not reachable.
     *
     * @return the level name, or null
     */
    public static String currentLevel() {
        try {
            Object level = Class.forName(DETECTOR_CLASS).getMethod("getLevel").invoke(null);
            return level == null ? null : level.toString();
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * Sets the detection level.
     *
     * @param name the level name, in any case
     * @return true if the level was applied, false if it was not a level or Netty is not reachable
     */
    public static boolean setLevel(String name) {
        try {
            Class<?> levelClass = Class.forName(LEVEL_CLASS);

            // Matched against the enum constants rather than Enum.valueOf on an upper-cased
            // string: the input comes from chat, and case folding an identifier with the default
            // locale is how "PARANOID" stops matching under a Turkish locale.
            Object level = null;
            for (Object candidate : levelClass.getEnumConstants()) {
                if (candidate.toString().equalsIgnoreCase(name)) {
                    level = candidate;
                    break;
                }
            }
            if (level == null) {
                return false;
            }

            Class.forName(DETECTOR_CLASS).getMethod("setLevel", levelClass).invoke(null, level);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * The levels this Netty build offers, lowest first, or an empty array if unreachable.
     *
     * @return the level names
     */
    public static String[] levels() {
        try {
            Object[] constants = Class.forName(LEVEL_CLASS).getEnumConstants();
            String[] names = new String[constants.length];
            for (int i = 0; i < constants.length; i++) {
                names[i] = constants[i].toString();
            }
            return names;
        } catch (Throwable t) {
            return new String[0];
        }
    }
}
