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

package me.lucko.spark.common.command;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * fork - pins the behaviour of a flag given without a value.
 *
 * <p>A bare flag parses to an empty string rather than to no value at all, which is why
 * {@link Arguments#intFlag} never reaches its -1 "undefined" return for one: it parses the empty
 * string and throws instead. Commands that advertise a bare form as the default - {@code --watch},
 * {@code --investigate} - have to account for that themselves, and this is the property they rely
 * on.</p>
 */
class BareFlagTest {

    @Test
    void aBareFlagIsPresentButCarriesAnEmptyValue() {
        Arguments arguments = new Arguments(Collections.singletonList("--watch"), false);

        assertTrue(arguments.boolFlag("watch"));
        assertEquals(Collections.singleton(""), arguments.stringFlag("watch"));
    }

    @Test
    void intFlagThrowsForABareFlagRatherThanReturningUndefined() {
        Arguments arguments = new Arguments(Collections.singletonList("--watch"), false);

        // the -1 sentinel is unreachable here - this is the trap the callers work around
        assertThrows(Arguments.ParseException.class, () -> arguments.intFlag("watch"));
    }

    @Test
    void aFlagWithAValueParsesNormally() {
        Arguments arguments = new Arguments(Arrays.asList("--watch", "30"), false);

        assertEquals(Collections.singleton("30"), arguments.stringFlag("watch"));
        assertEquals(30, arguments.intFlag("watch"));
    }
}
