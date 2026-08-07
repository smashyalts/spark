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

import me.lucko.spark.common.sampler.source.ClassSourceLookup;

import org.geysermc.geyser.api.GeyserApi;
import org.geysermc.geyser.api.extension.Extension;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Attributes sampled classes back to the Geyser extension that loaded them.
 *
 * <p>Each Geyser extension gets its own classloader, so mapping classloader to
 * extension id is enough to tell "this hot method belongs to extension X" apart
 * from Geyser's own code.</p>
 */
public class GeyserClassSourceLookup extends ClassSourceLookup.ByClassLoader {

    private final Map<ClassLoader, String> loaderToExtension;

    public GeyserClassSourceLookup() {
        this.loaderToExtension = new HashMap<>();
        for (Extension extension : GeyserApi.api().extensionManager().extensions()) {
            ClassLoader loader = extension.getClass().getClassLoader();
            if (loader != null) {
                this.loaderToExtension.put(loader, extension.description().id());
            }
        }
    }

    @Override
    public @Nullable String identify(ClassLoader loader) {
        return this.loaderToExtension.get(loader);
    }
}
