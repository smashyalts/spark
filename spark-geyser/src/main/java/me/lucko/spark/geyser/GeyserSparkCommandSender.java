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

import me.lucko.spark.common.command.sender.AbstractCommandSender;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.ansi.ANSIComponentSerializer;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;

import org.geysermc.geyser.api.command.CommandSource;

import java.util.UUID;

/**
 * Geyser's {@link CommandSource#sendMessage(String)} takes a plain String rather
 * than an Adventure {@link Component}, so spark's output has to be serialized here.
 *
 * <p>Console output is serialized to ANSI (matching what the standalone agent does)
 * so colours survive in a terminal; Bedrock players get section-sign legacy codes,
 * which is what Geyser's own commands emit.</p>
 */
public class GeyserSparkCommandSender extends AbstractCommandSender<CommandSource> {

    public GeyserSparkCommandSender(CommandSource source) {
        super(source);
    }

    @Override
    public String getName() {
        if (super.delegate.isConsole()) {
            return "Console";
        }
        return super.delegate.name();
    }

    @Override
    public UUID getUniqueId() {
        if (super.delegate.isConsole()) {
            return null;
        }
        return super.delegate.playerUuid();
    }

    @Override
    public void sendMessage(Component message) {
        if (super.delegate.isConsole()) {
            super.delegate.sendMessage(ANSIComponentSerializer.ansi().serialize(message));
        } else {
            super.delegate.sendMessage(LegacyComponentSerializer.legacySection().serialize(message));
        }
    }

    @Override
    public boolean hasPermission(String permission) {
        return super.delegate.hasPermission(permission);
    }
}
