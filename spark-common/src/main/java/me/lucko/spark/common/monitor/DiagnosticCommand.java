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

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;

/**
 * fork - runs jcmd diagnostic commands against this JVM without a shell.
 *
 * <p>The JVM registers a {@code DiagnosticCommandMBean} on the platform MBean server under
 * {@code com.sun.management:type=DiagnosticCommand}, and every jcmd subcommand appears there as
 * an MBean operation. That makes VM.native_memory, GC.heap_info, VM.classloader_stats and
 * System.trim_native_heap reachable from inside the process, with no attach API, no jcmd binary
 * on the PATH and no exec into the container.</p>
 *
 * <p>The MBean operation name is derived from the command name: everything up to the first dot
 * is lower-cased, then the dot is dropped and the following character upper-cased, and each
 * underscore is dropped with its following character upper-cased. VM.native_memory becomes
 * vmNativeMemory. This transformation is implementation-defined rather than specified, so
 * {@link #execute} treats a missing operation as an unavailable command rather than an error.</p>
 */
public enum DiagnosticCommand {
    ;

    private static final String DIAGNOSTIC_BEAN = "com.sun.management:type=DiagnosticCommand";
    private static final String HOTSPOT_BEAN = "com.sun.management:type=HotSpotDiagnostic";

    /** Prefix used on the return value when a command could not be run at all. */
    public static final String UNAVAILABLE = "unavailable: ";

    /** Converts a jcmd command name to its MBean operation name. */
    public static String operationName(String command) {
        StringBuilder sb = new StringBuilder(command.length());
        boolean seenDot = false;
        boolean upperNext = false;

        for (int i = 0; i < command.length(); i++) {
            char c = command.charAt(i);
            if (c == '.' && !seenDot) {
                seenDot = true;
                upperNext = true;
            } else if (c == '_') {
                upperNext = true;
            } else if (upperNext) {
                sb.append(Character.toUpperCase(c));
                upperNext = false;
            } else {
                sb.append(seenDot ? c : Character.toLowerCase(c));
            }
        }
        return sb.toString();
    }

    /**
     * Executes a diagnostic command and returns its own text output.
     *
     * <p>Never throws. A failure returns a string starting with {@link #UNAVAILABLE}, because
     * every caller is itself a diagnostic and should degrade rather than take down the command
     * that invoked it.</p>
     */
    public static String execute(String command, String... args) {
        try {
            MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
            Object result = beanServer.invoke(
                    ObjectName.getInstance(DIAGNOSTIC_BEAN),
                    operationName(command),
                    new Object[]{args},
                    new String[]{"[Ljava.lang.String;"}
            );
            return result == null ? "" : result.toString();
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return UNAVAILABLE + cause.getClass().getSimpleName()
                    + (cause.getMessage() == null ? "" : " - " + cause.getMessage());
        }
    }

    /**
     * True if Native Memory Tracking is enabled.
     *
     * <p>NMT needs {@code -XX:NativeMemoryTracking=summary} at JVM startup and cannot be turned
     * on later, so this is worth checking before offering NMT output - the alternative is
     * printing the JVM's own refusal message and leaving the operator to work out why.</p>
     */
    public static boolean isNativeMemoryTrackingEnabled() {
        // Cached because the only way to ask is to run a full summary, and NMT cannot be switched
        // on after startup - so the answer is fixed for the life of the process and there is no
        // reason to pay for it on every invocation.
        Boolean cached = nmtEnabled;
        if (cached == null) {
            String output = execute("VM.native_memory", "summary");
            cached = !output.startsWith(UNAVAILABLE) && !output.contains("Native memory tracking is not enabled");
            nmtEnabled = cached;
        }
        return cached;
    }

    private static volatile Boolean nmtEnabled;

    /**
     * Committed bytes for one NMT category, or -1 if unavailable.
     *
     * <p>Needed because the obvious estimates are wrong by large margins. Thread stacks are the
     * clearest case: multiplying the thread count by the default stack size measures RESERVED
     * address space, but only touched pages are resident, and on a server with several hundred
     * threads the difference is most of a gigabyte. Attributing that phantom to the JVM makes
     * everything unattributed look correspondingly smaller.</p>
     */
    public static long getNmtCommitted(String category) {
        String summary = execute("VM.native_memory", "summary");
        if (summary.startsWith(UNAVAILABLE) || summary.contains("not enabled")) {
            return -1;
        }
        for (String line : summary.split("\n")) {
            String trimmed = line.trim();
            if (!trimmed.startsWith("-") || !trimmed.contains(category)) {
                continue;
            }
            java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("committed=(\\d+)([KMG]?)B").matcher(trimmed);
            if (m.find()) {
                long value = Long.parseLong(m.group(1));
                String unit = m.group(2);
                if ("K".equals(unit)) {
                    return value * 1024L;
                } else if ("M".equals(unit)) {
                    return value * 1024L * 1024L;
                } else if ("G".equals(unit)) {
                    return value * 1024L * 1024L * 1024L;
                }
                return value;
            }
        }
        return -1;
    }

    /** Reads the current value of a -XX option, or null if it cannot be read. */
    public static String getVmOption(String option) {
        try {
            MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
            Object result = beanServer.invoke(
                    ObjectName.getInstance(HOTSPOT_BEAN),
                    "getVMOption",
                    new Object[]{option},
                    new String[]{"java.lang.String"}
            );
            if (result instanceof CompositeData) {
                Object value = ((CompositeData) result).get("value");
                return value == null ? null : value.toString();
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }
}
