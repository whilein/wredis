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
public interface RedisConfigBuilder {

    static @NotNull RedisConfigBuilder create(final @NotNull InetSocketAddress address) {
        return new Default(address);
    }

    @NotNull RedisConfigBuilder auth(@NotNull String username, @NotNull String password);

    @NotNull RedisConfigBuilder auth(@NotNull String password);

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
    @NotNull RedisConfigBuilder asciiWriter(@NotNull AsciiWriter asciiWriter);

    /**
     * Сменить изначальный размер буфера записи
     * <p>
     * По умолчанию изначальный размер равен {@code 1024}.
     *
     * @param capacity новый изначальный размер буфера записи
     * @return {@code this}
     */
    @NotNull RedisConfigBuilder writeBufferCapacity(int capacity);

    /**
     * Сменить изначальный размер буфера чтения.
     * <p>
     * По умолчанию изначальный размер равен {@code 1024}.
     *
     * @param capacity новый изначальный размер буфера чтения
     * @return {@code this}
     */
    @NotNull RedisConfigBuilder readBufferCapacity(int capacity);

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
    @NotNull RedisConfigBuilder connectTimeout(long timeout, @NotNull TimeUnit timeUnit);

    /**
     * Изменить опцию {@code TCP_NODELAY} для канала.
     * <p>
     * По умолчанию значение равно {@code false}.
     *
     * @param tcpNoDelay новое значение опции {@code TCP_NODELAY}
     * @return {@code this}
     */
    @NotNull RedisConfigBuilder tcpNoDelay(boolean tcpNoDelay);

    @NotNull RedisConfig build();

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    final class Default implements RedisConfigBuilder {

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
        long timeout;

        @NonFinal
        boolean tcpNoDelay;

        @Override
        public @NotNull RedisConfigBuilder auth(
                final @NotNull String username,
                final @NotNull String password
        ) {
            this.username = username;
            this.password = password;

            return this;
        }

        @Override
        public @NotNull RedisConfigBuilder auth(final @NotNull String password) {
            this.username = null;
            this.password = password;

            return this;
        }

        @Override
        public @NotNull RedisConfigBuilder asciiWriter(final @NotNull AsciiWriter asciiWriter) {
            this.asciiWriter = asciiWriter;

            return this;
        }

        @Override
        public @NotNull RedisConfigBuilder writeBufferCapacity(final int capacity) {
            this.writeCapacity = capacity;

            return this;
        }

        @Override
        public @NotNull RedisConfigBuilder readBufferCapacity(final int capacity) {
            this.readCapacity = capacity;

            return this;
        }

        @Override
        public @NotNull RedisConfigBuilder connectTimeout(final long timeout, final @NotNull TimeUnit unit) {
            this.timeout = unit.toMillis(timeout);

            return this;
        }

        @Override
        public @NotNull RedisConfigBuilder tcpNoDelay(final boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;

            return this;
        }

        @Override
        public @NotNull RedisConfig build() {
            return RedisConfig.create(
                    address,
                    writeCapacity == null ? 1024 : writeCapacity,
                    readCapacity == null ? 1024 : readCapacity,
                    timeout,
                    tcpNoDelay,
                    asciiWriter == null ? AsciiWriter.defaultAsciiWriter() : asciiWriter,
                    username,
                    password
            );
        }
    }

}
