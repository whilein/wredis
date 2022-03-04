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
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author whilein
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
public final class RedisConfigBuilder {

    InetSocketAddress address;

    @NonFinal
    String username;

    @NonFinal
    String password;

    @NonFinal
    AsciiWriter asciiWriter;

    @NonFinal
    Integer writeCapacity;

    @NonFinal
    Integer readCapacity;

    @NonFinal
    Integer soRcvBuf;

    @NonFinal
    Integer soSndBuf;

    @NonFinal
    long timeout;

    @NonFinal
    boolean tcpNoDelay;

    public @NotNull RedisConfigBuilder auth(
            final @NotNull String username,
            final @NotNull String password
    ) {
        this.username = username;
        this.password = password;

        return this;
    }

    public @NotNull RedisConfigBuilder auth(final @NotNull String password) {
        this.username = null;
        this.password = password;

        return this;
    }

    /**
     * Сменить {@link AsciiWriter}.
     * <p>
     * По умолчанию используется {@link AsciiWriter#defaultAsciiWriter()}.
     * <p>
     * Смотрите описание {@link AsciiWriter}, зачем это нужно.
     *
     * @param asciiWriter новый {@link AsciiWriter}
     * @return {@code this}
     */
    public @NotNull RedisConfigBuilder asciiWriter(final @NotNull AsciiWriter asciiWriter) {
        this.asciiWriter = asciiWriter;

        return this;
    }

    /**
     * Сменить значение опции {@link java.net.StandardSocketOptions#SO_RCVBUF} на значение {@code value}
     * <p>
     * По умолчанию значение равно {@code 1024}.
     *
     * @param value новое значение опции
     * @return {@code this}
     */
    public @NotNull RedisConfigBuilder soRcvBuf(final int value) {
        this.soRcvBuf = value;
        return this;
    }

    /**
     * Сменить значение опции {@link java.net.StandardSocketOptions#SO_SNDBUF} на значение {@code value}
     * <p>
     * По умолчанию значение равно {@code 1024}.
     *
     * @param value новое значение опции
     * @return {@code this}
     */
    public @NotNull RedisConfigBuilder soSndBuf(final int value) {
        this.soSndBuf = value;
        return this;
    }

    /**
     * Сменить изначальный размер буфера записи
     * <p>
     * По умолчанию изначальный размер равен {@code 1024}.
     *
     * @param capacity новый изначальный размер буфера записи
     * @return {@code this}
     */
    public @NotNull RedisConfigBuilder writeBufferCapacity(final int capacity) {
        this.writeCapacity = capacity;

        return this;
    }

    /**
     * Сменить изначальный размер буфера чтения.
     * <p>
     * По умолчанию изначальный размер равен {@code 1024}.
     *
     * @param capacity новый изначальный размер буфера чтения
     * @return {@code this}
     */
    public @NotNull RedisConfigBuilder readBufferCapacity(final int capacity) {
        this.readCapacity = capacity;

        return this;
    }

    /**
     * Изменить таймаут подключения.
     * <p>
     * По умолчанию таймаут равен {@code 0}, т.е. ожидание подключения будет
     * вечным, следуя документации {@link java.nio.channels.Selector#select(long)}.
     *
     * @param timeout  таймаут
     * @param timeUnit единица времени, в которой измеряется таймаут
     * @return {@code this}
     */
    public @NotNull RedisConfigBuilder connectTimeout(final long timeout, final @NotNull TimeUnit timeUnit) {
        this.timeout = timeUnit.toMillis(timeout);

        return this;
    }

    /**
     * Изменить опцию {@code TCP_NODELAY} для канала.
     * <p>
     * По умолчанию значение равно {@code false}.
     *
     * @param tcpNoDelay новое значение опции {@code TCP_NODELAY}
     * @return {@code this}
     */
    public @NotNull RedisConfigBuilder tcpNoDelay(final boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;

        return this;
    }

    public @NotNull RedisConfig build() {
        return new RedisConfig(
                address,
                writeCapacity == null ? 1024 : writeCapacity,
                readCapacity == null ? 1024 : readCapacity,
                soSndBuf == null ? 1024 : soSndBuf,
                soRcvBuf == null ? 1024 : soRcvBuf,
                timeout,
                tcpNoDelay,
                asciiWriter == null ? AsciiWriter.defaultAsciiWriter() : asciiWriter,
                username,
                password
        );
    }
}