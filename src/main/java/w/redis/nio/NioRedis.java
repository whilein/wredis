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
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import w.redis.Redis;
import w.redis.RedisCommand;
import w.redis.RedisResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/**
 * @author whilein
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NioRedis implements Redis {

    InetSocketAddress address;

    DynBuffer write;

    DynBuffer read;

    @NonFinal
    NioRedisSession session;

    @Override
    public boolean isAvailable() {
        try {
            ensureConnection();

            return session != null && session.isConnected();
        } catch (final IOException e) {
            return false;
        }
    }

    public static @NotNull Redis create(final @NotNull InetAddress address, final int port) {
        return create(new InetSocketAddress(address, port));
    }

    public static @NotNull Redis create(final @NotNull InetSocketAddress address) {
        return new NioRedis(
                address,
                new DynBuffer(ByteBuffer.allocate(1024)),
                new DynBuffer(ByteBuffer.allocate(1024))
        );
    }

    private void ensureConnection() throws IOException {
        if (session == null || !session.isConnected()) {
            val channel = SocketChannel.open(address);
            channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);
            channel.configureBlocking(false);

            val selector = Selector.open();
            channel.register(selector, SelectionKey.OP_READ, null);

            session = new NioRedisSession(channel, selector);
        }
    }

    @Override
    public void auth(final @NotNull String username, final @NotNull String password) {
        command("AUTH", 2)
                .argument(username)
                .argument(password);

        flush();
    }

    @Override
    public @NotNull RedisCommand command(final @NotNull String name, final int arguments) {
        write.writeCommand(name, arguments);

        return new NioRedisCommand(write);
    }

    @Override
    public void command(final @NotNull String name) {
        write.writeNoArgCommand(name);
    }

    @Override
    @SneakyThrows
    public void flush() {
        ensureConnection();

        val buffer = write.buffer;
        buffer.flip();

        val channel = session.channel;
        channel.write(buffer);

        buffer.clear();
    }

    private void awaitReadable() throws IOException {
        val selector = session.selector;
        selector.select(0);

        val readyKeys = selector.selectedKeys();
        readyKeys.removeIf(SelectionKey::isReadable);
    }

    @Override
    @SneakyThrows
    public @NotNull RedisResponse read() {
        ensureConnection();

        ByteBuffer buffer = read.buffer;
        buffer.clear();

        awaitReadable();

        val channel = session.channel;

        while (channel.read(buffer) > 0) {
            if (buffer.position() == buffer.capacity()) {
                buffer = read.resize();
            }
        }

        buffer.flip();

        return NioRedisResponse.create(buffer);
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            session.channel.close();
            session.selector.close();
            session = null;
        }
    }

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class NioRedisCommand implements RedisCommand {

        DynBuffer buffer;

        @Override
        public @NotNull RedisCommand argument(final int number) {
            buffer.writeNumber(number);

            return this;
        }

        @Override
        public @NotNull RedisCommand argument(final long number) {
            buffer.writeNumber(number);

            return this;
        }

        @Override
        public @NotNull RedisCommand argument(final @NotNull String text) {
            buffer.write(text);

            return this;
        }

        @Override
        public @NotNull RedisCommand argument(final byte @NotNull [] bytes) {
            buffer.write(bytes);

            return this;
        }

    }

    @AllArgsConstructor
    private static final class DynBuffer {
        ByteBuffer buffer;

        private void ensure(final int len) {
            final int requiredCapacity = buffer.position() + len;
            final int currentCapacity = buffer.capacity();

            if (requiredCapacity > currentCapacity) {
                resize(Math.min(requiredCapacity, currentCapacity * 2));
            }
        }

        public ByteBuffer resize() {
            return resize(buffer.capacity() * 2);
        }

        private ByteBuffer resize(final int to) {
            val newBuffer = ByteBuffer.allocate(to);
            buffer.flip();
            newBuffer.put(buffer);

            return this.buffer = newBuffer;
        }

        public void writeCRLF() {
            buffer.put((byte) '\r').put((byte) '\n');
        }

        private void writeNoArgCommand(final String command) {
            ensure(2);
            buffer.put((byte) '*').put((byte) '0');

            val commandLength = command.length();
            writeLength('$', commandLength);

            ensure(commandLength);
            writeAscii(command, commandLength);
            writeCRLF();
        }

        private void writeCommand(final String command, final int arguments) {
            writeLength('*', arguments + 1);
            val commandLength = command.length();
            writeLength('$', commandLength);

            ensure(commandLength);
            writeAscii(command, commandLength);
            writeCRLF();
        }

        public void writeAscii(final String ascii, final int len) {
            for (int i = 0; i < len; i++) {
                buffer.put((byte) ascii.charAt(i));
            }
        }

        public void writeLength(final char prefix, final int length) {
            val lengthAsString = String.valueOf(length);
            val lengthOfLength = lengthAsString.length();

            ensure(3 + lengthOfLength);
            buffer.put((byte) prefix);

            writeAscii(lengthAsString, lengthOfLength);
            writeCRLF();
        }

        private void writeAscii(final String ascii) {
            val length = ascii.length();

            writeLength('$', length);

            ensure(length + 2);
            writeAscii(ascii, length);
            writeCRLF();
        }

        public void writeNumber(final int number) {
            writeAscii(Integer.toString(number));
        }

        public void writeNumber(final long number) {
            writeAscii(Long.toString(number));
        }

        private void writeEmptyString() {
            ensure(4);

            buffer.put((byte) '$').put((byte) '0');
            writeCRLF();
        }

        public void write(final byte[] bytes) {
            val blobLength = bytes.length;

            if (blobLength == 0) {
                writeEmptyString();
                return;
            }

            writeLength('$', blobLength);

            ensure(blobLength + 2);
            buffer.put(bytes);
            writeCRLF();
        }

        public void write(final String text) {
            if (text.length() == 0) {
                writeEmptyString();
                return;
            }

            val bytes = text.getBytes(StandardCharsets.UTF_8);

            val textLength = bytes.length;
            writeLength('$', textLength);

            ensure(bytes.length + 2);
            buffer.put(bytes);
            writeCRLF();
        }
    }

    @FieldDefaults(makeFinal = true)
    @RequiredArgsConstructor
    private static final class NioRedisSession {
        SocketChannel channel;
        Selector selector;

        public boolean isConnected() {
            return channel.isConnected();
        }
    }

}
