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

package me.lucko.spark.common.util.log;

import java.util.logging.Level;

/**
 * Special logger for use by classes that don't easily have access to a
 * {@link me.lucko.spark.common.SparkPlatform} instance.
 *
 * <p>This avoids warnings on platforms like Paper that get upset if plugins use
 * {@link System#out} or {@link System#err}.</p>
 */
public enum SparkStaticLogger {
    ;

    private static volatile Logger logger = Logger.FALLBACK;

    /**
     * Installs the platform logger, if one has not been installed already.
     *
     * <p>The check is against {@link Logger#FALLBACK} rather than {@code null}: the field starts
     * out holding the fallback, so a null check never passes and the platform logger is never
     * installed - leaving every static log line going to {@link System#out}/{@link System#err},
     * which is exactly what this class exists to avoid.</p>
     *
     * @param logger the logger to install
     */
    public synchronized static void setLogger(Logger logger) {
        if (logger != null && SparkStaticLogger.logger == Logger.FALLBACK) {
            SparkStaticLogger.logger = logger;
        }
    }

    public static void log(Level level, String msg, Throwable throwable) {
        logger.log(level, msg, throwable);
    }

    public static void log(Level level, String msg) {
        logger.log(level, msg);
    }

}
