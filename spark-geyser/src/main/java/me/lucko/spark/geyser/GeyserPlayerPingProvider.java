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

import me.lucko.spark.common.monitor.ping.PlayerPingProvider;

import org.geysermc.geyser.api.GeyserApi;
import org.geysermc.geyser.api.connection.GeyserConnection;

import java.util.HashMap;
import java.util.Map;

/**
 * Reports the Bedrock-side RakNet ping of each connected Geyser session.
 *
 * <p>Note this is the Bedrock client to Geyser leg only. It says nothing about
 * the Geyser to Java server leg, so a healthy number here does not rule out
 * latency introduced downstream.</p>
 */
public class GeyserPlayerPingProvider implements PlayerPingProvider {

    @Override
    public Map<String, Integer> poll() {
        Map<String, Integer> pings = new HashMap<>();
        for (GeyserConnection connection : GeyserApi.api().onlineConnections()) {
            pings.put(connection.name(), connection.ping());
        }
        return pings;
    }
}
