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

import me.lucko.spark.common.platform.PlatformInfo;

import org.geysermc.geyser.api.GeyserApi;
import org.geysermc.geyser.api.util.MinecraftVersion;

public class GeyserPlatformInfo implements PlatformInfo {

    @Override
    public Type getType() {
        // Geyser translates between Bedrock and Java editions - it sits in front of
        // a server rather than being one, so PROXY is the closest fit (same as Velocity).
        return Type.PROXY;
    }

    @Override
    public String getName() {
        return "Geyser";
    }

    @Override
    public String getBrand() {
        try {
            // e.g. "Standalone", "Spigot", "Velocity" - tells you which flavour of
            // Geyser produced the profile, which matters when reading the thread list.
            return "Geyser (" + GeyserApi.api().platformType().platformName() + ")";
        } catch (Throwable e) {
            return "Geyser";
        }
    }

    @Override
    public String getVersion() {
        // GeyserImpl.VERSION lives in Geyser core, not the extension API, so it has
        // to be read reflectively. Fall back to the API version if that ever moves.
        try {
            Class<?> geyserImpl = Class.forName("org.geysermc.geyser.GeyserImpl");
            Object version = geyserImpl.getField("VERSION").get(null);
            if (version != null) {
                return version.toString();
            }
        } catch (Throwable e) {
            // ignore, fall through
        }

        try {
            return "api-" + GeyserApi.api().geyserApiVersion();
        } catch (Throwable e) {
            return "unknown";
        }
    }

    @Override
    public String getMinecraftVersion() {
        try {
            MinecraftVersion version = GeyserApi.api().supportedJavaVersion();
            return version == null ? null : version.versionString();
        } catch (Throwable e) {
            return null;
        }
    }
}
