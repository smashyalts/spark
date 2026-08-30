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

package me.lucko.spark.common.ws;

import me.lucko.spark.common.util.config.Configuration;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A store of trusted public keys.
 */
public class TrustedKeyStore {
    private static final String TRUSTED_KEYS_OPTION = "trustedKeys";

    /** The spark configuration */
    private final Configuration configuration;
    /** Gets the local public/private key */
    private final CompletableFuture<KeyPair> localKeyPair;
    /** A set of remote public keys to trust */
    // trusted from a command thread, consulted from the websocket listener thread
    private final Set<PublicKey> remoteTrustedKeys;
    /** A map of pending remote public keys */
    // added from the websocket listener thread, removed from a command thread
    private final Map<String, PublicKey> remotePendingKeys = new ConcurrentHashMap<>();

    public TrustedKeyStore(Configuration configuration) {
        this.configuration = configuration;
        this.localKeyPair = CompletableFuture.supplyAsync(ViewerSocketConnection.CRYPTO::generateKeyPair);
        this.remoteTrustedKeys = ConcurrentHashMap.newKeySet();
        readTrustedKeys();
    }

    /**
     * Gets the local public key.
     *
     * @return the local public key
     */
    public PublicKey getLocalPublicKey() {
        return this.localKeyPair.join().getPublic();
    }

    /**
     * Gets the local private key.
     *
     * @return the local private key
     */
    public PrivateKey getLocalPrivateKey() {
        return this.localKeyPair.join().getPrivate();
    }

    /**
     * Checks if a remote public key is trusted
     *
     * @param publicKey the public key
     * @return if the key is trusted
     */
    public boolean isKeyTrusted(PublicKey publicKey) {
        return publicKey != null && this.remoteTrustedKeys.contains(publicKey);
    }

    /**
     * Adds a pending public key to be trusted in the future.
     *
     * @param clientId the client id submitting the key
     * @param publicKey the public key
     */
    public void addPendingKey(String clientId, PublicKey publicKey) {
        this.remotePendingKeys.put(clientId, publicKey);
    }

    /**
     * Trusts a previously submitted remote public key
     *
     * @param clientId the id of the client that submitted the key
     * @return true if the key was found and trusted
     */
    public boolean trustPendingKey(String clientId) {
        PublicKey key = this.remotePendingKeys.remove(clientId);
        if (key == null) {
            return false;
        }

        this.remoteTrustedKeys.add(key);
        writeTrustedKeys();
        return true;
    }

    /**
     * Reads trusted keys from the configuration
     */
    private void readTrustedKeys() {
        for (String encodedKey : this.configuration.getStringList(TRUSTED_KEYS_OPTION)) {
            try {
                PublicKey publicKey = ViewerSocketConnection.CRYPTO.decodePublicKey(Base64.getDecoder().decode(encodedKey));
                this.remoteTrustedKeys.add(publicKey);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Writes trusted keys to the configuration
     */
    private void writeTrustedKeys() {
        List<String> encodedKeys = this.remoteTrustedKeys.stream()
                .map(key -> Base64.getEncoder().encodeToString(key.getEncoded()))
                .collect(Collectors.toList());

        // Re-read from disk first. The file configuration is loaded once when the plugin is
        // enabled and never again, so saving it as-is would serialise a snapshot that is however
        // many hours stale over the file - silently undoing any edit an admin made to it while
        // the server was running.
        this.configuration.load();
        this.configuration.setStringList(TRUSTED_KEYS_OPTION, encodedKeys);
        // without this the updated list only ever exists in memory, so a key the user explicitly
        // trusted has to be trusted again after every restart
        this.configuration.save();
    }

}
