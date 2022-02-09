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
import w.redis.AsciiWriter;
import w.redis.Redis;
import w.redis.RedisResponse;
import w.redis.util.NumberUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author whilein
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NioRedis implements Redis {

    InetSocketAddress address;

    DynWriteBuffer write;

    DynReadBuffer read;

    @NonFinal
    NioRedisSession session;

    RedisResponse response;

    @Override
    public boolean isAvailable() {
        try {
            ensureConnection();

            return session != null && session.isConnected();
        } catch (final IOException e) {
            return false;
        }
    }

    @Override
    public void setAsciiWriter(final @NotNull AsciiWriter asciiWriter) {
        write.asciiWriter = asciiWriter;
    }

    public static @NotNull Redis create(final @NotNull InetAddress address, final int port) {
        return create(new InetSocketAddress(address, port));
    }

    public static @NotNull Redis create(final @NotNull InetSocketAddress address) {
        return new NioRedis(
                address,
                new DynWriteBuffer(ByteBuffer.allocate(8192), AsciiWriter.defaultAsciiWriter()),
                new DynReadBuffer(ByteBuffer.allocate(8192)),
                NioRedisResponse.create()
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
    public @NotNull Redis command(final @NotNull String name, final int arguments) {
        write.writeCommand(name, arguments);

        return this;
    }

    @Override
    public @NotNull Redis argument(final int number) {
        write.writeNumber(number);

        return this;
    }

    @Override
    public @NotNull Redis argument(final long number) {
        write.writeNumber(number);

        return this;
    }

    @Override
    public @NotNull Redis argument(final @NotNull String text) {
        return argument(text, StandardCharsets.UTF_8);
    }

    @Override
    public @NotNull Redis argument(final @NotNull String text, final @NotNull Charset charset) {
        write.writeText(text, charset);

        return this;
    }

    @Override
    public @NotNull Redis argument(final byte @NotNull [] bytes) {
        write.writeBytes(bytes);

        return this;
    }

    @Override
    public @NotNull Redis command(final @NotNull String name) {
        write.writeNoArgCommand(name);

        return this;
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
    public @NotNull RedisResponse flushAndRead() {
        flush();
        return read();
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

        response.setBuffer(buffer);

        return response;
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            session.channel.close();
            session.selector.close();
            session = null;
        }
    }

    @AllArgsConstructor
    private static abstract class DynBuffer {
        ByteBuffer buffer;

        public ByteBuffer resize() {
            return resize(buffer.capacity() * 2);
        }

        public ByteBuffer resize(final int to) {
            val newBuffer = ByteBuffer.allocate(to);
            buffer.flip();
            newBuffer.put(buffer);

            return this.buffer = newBuffer;
        }

    }

    private static final class DynReadBuffer extends DynBuffer {

        public DynReadBuffer(final ByteBuffer buffer) {
            super(buffer);
        }

    }

    private static final class DynWriteBuffer extends DynBuffer {

        AsciiWriter asciiWriter;

        public DynWriteBuffer(final ByteBuffer buffer, final AsciiWriter asciiWriter) {
            super(buffer);

            this.asciiWriter = asciiWriter;
        }

        private void ensure(final int len) {
            final int requiredCapacity = buffer.position() + len;
            final int currentCapacity = buffer.capacity();

            if (requiredCapacity > currentCapacity) {
                resize(Math.min(requiredCapacity, currentCapacity * 2));
            }
        }


        public void writeCrlf() {
            buffer.put((byte) '\r').put((byte) '\n');
        }

        public void writeNoArgCommand(final String command) {
            ensure(4);
            buffer.put((byte) '*').put((byte) '1');
            writeCrlf();

            val commandLength = command.length();
            writeLength('$', commandLength);

            ensure(commandLength);
            writeAscii(command);
            writeCrlf();
        }

        public void writeCommand(final String command, final int arguments) {
            writeLength('*', arguments + 1);
            val commandLength = command.length();
            writeLength('$', commandLength);

            ensure(commandLength);
            writeAscii(command);
            writeCrlf();
        }

        public void writeAscii(final String ascii) {
            asciiWriter.write(ascii, buffer);
        }

        private int writeLong(int position, long value) {
            buffer.position(position);

            while (value > 0) {
                buffer.put(--position, (byte) ((byte) (value % 10) + '0'));
                value /= 10;
            }

            return position;
        }


        private int writeInt(int position, int value) {
            buffer.position(position);

            while (value > 0) {
                buffer.put(--position, (byte) ((byte) (value % 10) + '0'));
                value /= 10;
            }

            return position;
        }

        public void writeLength(final char prefix, final int length) {
            val lengthOfNumber = NumberUtils.getIntLength(length);

            ensure(3 + lengthOfNumber);
            buffer.put((byte) prefix);

            val position = buffer.position() + lengthOfNumber;
            writeInt(position, length);
            writeCrlf();
        }

        public void writeNumber(final int rawNumber) {
            val negative = rawNumber < 0;

            final int number;
            final int length;

            if (negative) {
                length = NumberUtils.getIntLength(number = -rawNumber) + 1;
            } else {
                length = NumberUtils.getIntLength(number = rawNumber);
            }

            writeLength('$', length);

            ensure(length + 2);

            val position = buffer.position() + length;
            val lastPosition = writeInt(position, number);

            if (negative) {
                buffer.put(lastPosition - 1, (byte) '-');
            }

            writeCrlf();
        }

        public void writeNumber(final long rawNumber) {
            val negative = rawNumber < 0;

            final long number;
            final int length;

            if (negative) {
                length = NumberUtils.getLongLength(number = -rawNumber) + 1;
            } else {
                length = NumberUtils.getLongLength(number = rawNumber);
            }

            writeLength('$', length);

            ensure(length + 2);

            val position = buffer.position() + length;
            val lastPosition = writeLong(position, number);

            if (negative) {
                buffer.put(lastPosition - 1, (byte) '-');
            }

            writeCrlf();
        }

        private void writeEmptyString() {
            ensure(4);

            buffer.put((byte) '$').put((byte) '0');
            writeCrlf();
        }

        public void writeBytes(final byte[] bytes) {
            val blobLength = bytes.length;

            if (blobLength == 0) {
                writeEmptyString();
                return;
            }

            writeLength('$', blobLength);

            ensure(blobLength + 2);
            buffer.put(bytes);
            writeCrlf();
        }

        public void writeText(final String text, final Charset cs) {
            if (text.length() == 0) {
                writeEmptyString();
                return;
            }

            if (cs == StandardCharsets.US_ASCII) {
                val textLength = text.length();
                writeLength('$', textLength);

                ensure(textLength + 2);
                writeAscii(text);
            } else {
                val bytes = text.getBytes(cs);

                val textLength = bytes.length;
                writeLength('$', textLength);

                ensure(textLength + 2);
                buffer.put(bytes);
            }

            writeCrlf();
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
