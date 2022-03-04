/*
 *    Copyright 2022 Whilein
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package w.redis;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

/**
 * @author whilein
 */
public interface RedisConfig {

    @NotNull InetSocketAddress getAddress();

    int getReadBufferCapacity();

    int getWriteBufferCapacity();

    int getSoSndBuf();

    int getSoRcvBuf();

    long getConnectTimeoutMillis();

    boolean isTcpNoDelay();

    @NotNull AsciiWriter getAsciiWriter();

    @Nullable String getUsername();

    @Nullable String getPassword();

    static @NotNull RedisConfigBuilder builder(
            final @NotNull InetSocketAddress address
    ) {
        return RedisConfigBuilder.create(address);
    }

    static @NotNull RedisConfig create(
            final @NotNull InetSocketAddress address,
            final int writeBufferCapacity,
            final int readBufferCapacity,
            final int soSndBuf,
            final int soRcvBuf,
            final long connectTimeoutMillis,
            final boolean tcpNoDelay,
            final @NotNull AsciiWriter asciiWriter,
            final @Nullable String username,
            final @Nullable String password
    ) {
        return new Default(
                address,
                writeBufferCapacity, readBufferCapacity,
                soSndBuf, soRcvBuf,
                connectTimeoutMillis, tcpNoDelay, asciiWriter,
                username, password
        );
    }

    @Getter
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    final class Default implements RedisConfig {
        InetSocketAddress address;
        int writeBufferCapacity;
        int readBufferCapacity;
        int soSndBuf;
        int soRcvBuf;
        long connectTimeoutMillis;
        boolean tcpNoDelay;
        AsciiWriter asciiWriter;
        String username;
        String password;
    }

}
