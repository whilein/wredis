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

package w.redis.nio;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import w.redis.AsciiWriter;
import w.redis.ByteAllocator;
import w.redis.Redis;
import w.redis.RedisAuthException;
import w.redis.RedisBuilder;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author whilein
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NioRedisBuilder implements RedisBuilder {

    InetSocketAddress address;

    @NonFinal
    String username;

    @NonFinal
    String password;

    @NonFinal
    AsciiWriter asciiWriter;

    @NonFinal
    ByteAllocator writeAllocator;

    @NonFinal
    ByteAllocator readAllocator;

    @NonFinal
    Integer writeCapacity;

    @NonFinal
    Integer readCapacity;

    @NonFinal
    long timeout;

    @NonFinal
    boolean tcpNoDelay;

    public static @NotNull RedisBuilder create(final @NotNull InetSocketAddress address) {
        return new NioRedisBuilder(address);
    }

    @Override
    public @NotNull RedisBuilder auth(
            final @NotNull String username,
            final @NotNull String password
    ) {
        this.username = username;
        this.password = password;

        return this;
    }

    @Override
    public @NotNull RedisBuilder auth(final @NotNull String password) {
        this.username = null;
        this.password = password;

        return this;
    }

    @Override
    public @NotNull RedisBuilder asciiWriter(final @NotNull AsciiWriter asciiWriter) {
        this.asciiWriter = asciiWriter;

        return this;
    }

    @Override
    public @NotNull RedisBuilder writeBufferAllocator(final @NotNull ByteAllocator byteAllocator) {
        this.writeAllocator = byteAllocator;

        return this;
    }

    @Override
    public @NotNull RedisBuilder readBufferAllocator(final @NotNull ByteAllocator byteAllocator) {
        this.readAllocator = byteAllocator;

        return this;
    }

    @Override
    public @NotNull RedisBuilder writeBufferCapacity(final int capacity) {
        this.writeCapacity = capacity;

        return this;
    }

    @Override
    public @NotNull RedisBuilder readBufferCapacity(final int capacity) {
        this.readCapacity = capacity;

        return this;
    }

    @Override
    public @NotNull RedisBuilder connectTimeout(final long timeout, final @NotNull TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);

        return this;
    }

    @Override
    public @NotNull RedisBuilder tcpNoDelay(final boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;

        return this;
    }

    @Override
    public @NotNull Redis connect() {
        val redis = NioRedis.create(
                address,
                asciiWriter == null ? AsciiWriter.defaultAsciiWriter() : asciiWriter,
                writeAllocator == null ? ByteAllocator.heapBufferAllocator() : writeAllocator,
                readAllocator == null ? ByteAllocator.heapBufferAllocator() : readAllocator,
                writeCapacity == null ? 1024 : writeCapacity,
                readCapacity == null ? 1024 : readCapacity,
                timeout,
                tcpNoDelay
        );

        redis.connect();

        if (password != null) {
            if (username != null) {
                redis.command("AUTH", 2)
                        .argument(username)
                        .argument(password);
            } else {
                redis.command("AUTH", 1)
                        .argument(password);
            }

            val response = redis.flushAndRead();

            if (response.isError()) {
                throw new RedisAuthException(response.nextString());
            }
        }

        return redis;
    }
}
