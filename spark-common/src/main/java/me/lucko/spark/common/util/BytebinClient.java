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

package me.lucko.spark.common.util;

import com.google.protobuf.AbstractMessageLite;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.zip.GZIPOutputStream;

/**
 * Utility for posting content to bytebin.
 *
 * @see <a href="https://github.com/lucko/bytebin">https://github.com/lucko/bytebin</a>
 */
public class BytebinClient {

    /** The bytebin URL */
    private final String url;
    /** The client user agent */
    private final String userAgent;

    public BytebinClient(String url, String userAgent) {
        this.url = url + (url.endsWith("/") ? "" : "/");
        this.userAgent = userAgent;
    }

    private Content postContent(String contentType, Consumer<OutputStream> consumer, String userAgentExtra) throws IOException {
        String userAgent = userAgentExtra != null
                ? this.userAgent + "/" + userAgentExtra
                : this.userAgent;

        URL url = new URL(this.url + "post");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(10));
            connection.setReadTimeout((int) TimeUnit.SECONDS.toMillis(10));

            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", contentType);
            connection.setRequestProperty("User-Agent", userAgent);
            connection.setRequestProperty("Content-Encoding", "gzip");

            connection.connect();
            try (OutputStream output = connection.getOutputStream()) {
                consumer.accept(output);
            }

            String key = connection.getHeaderField("Location");
            if (key == null) {
                // no Location header means the server rejected the upload - it says why in the
                // response, so carry that into the message rather than leaving the user with a
                // bare "Key not returned" for what is usually a rate limit or a size limit
                throw new IllegalStateException("Key not returned" + describeFailure(connection));
            }
            return new Content(key);
        } finally {
            // getInputStream() itself throws when the server answered with an error status, which
            // would replace whatever went wrong above with a bare IOException and lose the real
            // reason for the failure. The error body has already been read by describeFailure in
            // that case, so there is nothing left to do but let it go.
            try (InputStream in = connection.getInputStream()) {
                // just closing it
            } catch (IOException e) {
                // no readable response body
            }
            connection.disconnect();
        }
    }

    /**
     * Describes a failed request using the status code and the body the server sent with it.
     *
     * @param connection the failed connection
     * @return a description to append to an exception message, or an empty string
     */
    private static String describeFailure(HttpURLConnection connection) {
        StringBuilder sb = new StringBuilder();
        try {
            sb.append(" (http ").append(connection.getResponseCode()).append(")");
        } catch (IOException e) {
            return "";
        }

        InputStream errorStream = connection.getErrorStream();
        if (errorStream == null) {
            return sb.toString();
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream, StandardCharsets.UTF_8))) {
            // Bounded: the body of an error response is a sentence, but nothing stops a
            // misconfigured proxy from answering with a whole html page.
            //
            // readLine() rather than lines(): the stream wraps a read failure in an
            // UncheckedIOException, which is not an IOException and would therefore escape this
            // method and replace the exception it was called to describe - the exact failure this
            // whole method exists to prevent.
            StringBuilder body = new StringBuilder();
            for (int i = 0; i < 5 && body.length() < 500; i++) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                if (body.length() != 0) {
                    body.append(' ');
                }
                body.append(line);
            }

            String text = body.toString().trim();
            if (!text.isEmpty()) {
                sb.append(": ").append(text.length() > 500 ? text.substring(0, 500) : text);
            }
        } catch (IOException e) {
            // nothing more to add
        }

        return sb.toString();
    }

    public Content postContent(AbstractMessageLite<?, ?> proto, String contentType, String userAgentExtra) throws IOException {
        return postContent(contentType, outputStream -> {
            try (OutputStream out = new GZIPOutputStream(outputStream)) {
                proto.writeTo(out);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, userAgentExtra);
    }

    public Content postContent(AbstractMessageLite<?, ?> proto, String contentType) throws IOException {
        return postContent(proto, contentType, null);
    }

    public static final class Content {
        private final String key;

        Content(String key) {
            this.key = key;
        }

        public String key() {
            return this.key;
        }
    }

}
