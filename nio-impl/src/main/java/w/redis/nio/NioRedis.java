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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import w.redis.AsciiWriter;
import w.redis.Redis;
import w.redis.RedisAuthException;
import w.redis.RedisConfig;
import w.redis.RedisResponse;
import w.redis.RedisSocketException;
import w.redis.util.NumberUtils;

import java.io.IOException;
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

    String username;
    String password;

    DynWriteBuffer write;

    DynReadBuffer read;

    RedisResponse response;

    long timeout;

    boolean tcpNoDelay;

    @Getter
    @NonFinal
    boolean closed;

    @NonFinal
    NioRedisSession session;

    public static @NotNull Redis create(
            final @NotNull RedisConfig config
    ) {
        return new NioRedis(
                config.getAddress(),
                config.getUsername(),
                config.getPassword(),
                new DynWriteBuffer(ByteBuffer.allocate(config.getWriteBufferCapacity()), config.getAsciiWriter()),
                new DynReadBuffer(ByteBuffer.allocate(config.getReadBufferCapacity())),
                NioRedisResponse.create(),
                config.getConnectTimeoutMillis(),
                config.isTcpNoDelay()
        );
    }

    private static void awaitReadable(final Selector selector) throws IOException {
        selector.select(0);

        val readyKeys = selector.selectedKeys();
        readyKeys.removeIf(SelectionKey::isReadable);
    }

    private static SelectionKey awaitConnectable(final Selector selector, final long timeout) throws IOException {
        selector.select(timeout);

        val readyKeys = selector.selectedKeys().iterator();

        while (readyKeys.hasNext()) {
            val key = readyKeys.next();

            if (key.isConnectable()) {
                readyKeys.remove();

                return key;
            }
        }

        return null;
    }

    @Override
    public @NotNull Redis connect() throws RedisSocketException {
        if (session == null || !session.isConnected()) {
            if (closed) {
                throw new IllegalStateException("Redis instance was closed");
            }

            try {
                val channel = SocketChannel.open();
                channel.setOption(StandardSocketOptions.TCP_NODELAY, tcpNoDelay);
                channel.configureBlocking(false);

                val selector = Selector.open();
                channel.register(selector, SelectionKey.OP_READ, null);
                channel.register(selector, SelectionKey.OP_CONNECT, null);

                channel.connect(address);

                val key = awaitConnectable(selector, timeout);

                if (key != null && channel.finishConnect()) {
                    key.interestOps(SelectionKey.OP_READ);

                    session = new NioRedisSession(channel, selector);

                    if (password != null) {
                        if (username != null) {
                            writeCommand("AUTH", 2)
                                    .writeUTF(username)
                                    .writeUTF(password);
                        } else {
                            writeCommand("AUTH", 1)
                                    .writeUTF(password);
                        }

                        val response = flushAndRead();

                        if (response.isError()) {
                            session = null;

                            throw new RedisAuthException(response.nextString());
                        }
                    }
                } else {
                    throw new RedisSocketException("Can't connect to " + address);
                }
            } catch (final IOException e) {
                throw new RedisSocketException("Can't connect to " + address, e);
            }
        }

        return this;
    }

    @Override
    public @NotNull Redis writeInt(final int number) {
        write.writeNumber(number);

        return this;
    }

    @Override
    public @NotNull Redis writeLong(final long number) {
        write.writeNumber(number);

        return this;
    }

    @Override
    public @NotNull Redis writeUTF(final @NotNull String text) {
        write.writeTextUTF(text);

        return this;
    }

    @Override
    public @NotNull Redis writeAscii(final @NotNull String text) {
        write.writeTextAscii(text);

        return this;
    }

    @Override
    public @NotNull Redis writeBytes(final byte @NotNull [] bytes) {
        write.writeBytes(bytes);

        return this;
    }

    @Override
    public @NotNull Redis writeCommand(final @NotNull String name) {
        write.writeNoArgCommand(name);

        return this;
    }

    @Override
    public @NotNull Redis writeCommand(final @NotNull String name, final int arguments) {
        write.writeCommand(name, arguments);

        return this;
    }

    private void _flush() throws IOException {
        val buffer = write.buffer;
        buffer.flip();

        val channel = session.channel;
        channel.write(buffer);

        buffer.clear();
    }

    @Override
    @SneakyThrows
    public void flush() {
        connect();

        _flush();
    }

    @Override
    @SneakyThrows
    public @NotNull RedisResponse flushAndRead() {
        connect();

        _flush();
        return _read();
    }

    private RedisResponse _read() throws IOException {
        ByteBuffer buffer = read.buffer;
        buffer.clear();

        awaitReadable(session.selector);

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
    @SneakyThrows
    public @NotNull RedisResponse read() {
        connect();

        return _read();
    }

    @Override
    @SneakyThrows
    public void close() {
        if (session != null) {
            closed = true;

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

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
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

        public void writeTextAscii(final String text) {
            if (text.length() == 0) {
                writeEmptyString();
                return;
            }

            val textLength = text.length();
            writeLength('$', textLength);

            ensure(textLength + 2);
            writeAscii(text);

            writeCrlf();
        }

        public void writeTextUTF(final String text) {
            if (text.length() == 0) {
                writeEmptyString();
                return;
            }

            val bytes = text.getBytes(StandardCharsets.UTF_8);

            val textLength = bytes.length;
            writeLength('$', textLength);

            ensure(textLength + 2);
            buffer.put(bytes);

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
