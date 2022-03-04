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
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import w.redis.buffer.ReadRedisBuffer;
import w.redis.buffer.WriteRedisBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author whilein
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Redis {

    InetSocketAddress address;

    String username;
    String password;

    int soSndBuf;
    int soRcvBuf;

    WriteRedisBuffer write;

    ReadRedisBuffer read;

    @NonFinal
    RedisResponse response;

    long timeout;

    boolean tcpNoDelay;

    @Getter
    @NonFinal
    boolean closed;

    @NonFinal
    RedisSession session;

    public static @NotNull Redis create(
            final @NotNull RedisConfig config
    ) {
        val redis = new Redis(
                config.getAddress(),
                config.getUsername(),
                config.getPassword(),
                config.getSoSndBuf(),
                config.getSoRcvBuf(),
                new WriteRedisBuffer(new byte[config.getWriteBufferCapacity()], 0, config.getAsciiWriter()),
                new ReadRedisBuffer(new byte[config.getReadBufferCapacity()], 0),
                config.getConnectTimeoutMillis(),
                config.isTcpNoDelay()
        );

        redis.response = new RedisResponse(redis, redis.read);

        return redis;
    }

    private void _connect() throws RedisSocketException {
        if (session == null || !session.isConnected()) {
            if (closed) {
                throw new IllegalStateException("Redis instance was closed");
            }

            try {
                val socket = new Socket();
                socket.setTcpNoDelay(tcpNoDelay);
                socket.setSendBufferSize(soSndBuf);
                socket.setReceiveBufferSize(soRcvBuf);
                socket.connect(address, (int) timeout);

                session = new RedisSession(
                        socket,
                        socket.getInputStream(),
                        socket.getOutputStream()
                );

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
            } catch (final IOException e) {
                throw new RedisSocketException("Can't connect to " + address, e);
            }
        }
    }

    public @NotNull Redis connect() throws RedisSocketException {
        _connect();

        return this;
    }

    public @NotNull Redis writeInt(final int number) {
        write.writeInt(number);

        return this;
    }

    public @NotNull Redis writeLong(final long number) {
        write.writeLong(number);

        return this;
    }

    public @NotNull Redis writeUTF(final @NotNull String text) {
        write.writeUTF(text);

        return this;
    }

    public @NotNull Redis writeAscii(final @NotNull String text) {
        write.writeAscii(text);

        return this;
    }

    public @NotNull Redis writeBytes(final byte @NotNull [] bytes) {
        write.writeBytes(bytes);

        return this;
    }

    public @NotNull Redis writeCommand(final @NotNull String name, final int arguments) {
        write.writeCommand(name, arguments);

        return this;
    }

    private void _flush() throws IOException {
        val buffer = write;

        val session = this.session;
        session.output.write(buffer.getArray(), 0, buffer.getPosition());

        buffer.setPosition(0);
    }

    @SneakyThrows
    public void flush() {
        _connect();
        _flush();
    }

    @SneakyThrows
    public @NotNull RedisResponse flushAndRead() {
        _connect();

        _flush();
        _read();

        return response;
    }

    private void _read() throws IOException {
        val input = session.input;

        val readBuffer = read;
        readBuffer.setPosition(0);
        readBuffer.setLength(0);

        val array = readBuffer.getArray();
        val arrayOffset = readBuffer.getPosition();

        val read = input.read(array, arrayOffset, array.length - arrayOffset);

        if (read == readBuffer.getCapacity()) {
            readBuffer.resize();
        }

        readBuffer.setLength(read);

        response.resetState();
    }

    @SneakyThrows
    public void readMore() {
        _read();
    }

    @SneakyThrows
    public @NotNull RedisResponse read() {
        _connect();
        _read();

        return response;
    }

    @SneakyThrows
    public void close() {
        if (session != null) {
            closed = true;

            session.socket.close();
            session = null;
        }
    }

    @FieldDefaults(makeFinal = true)
    @RequiredArgsConstructor
    private static final class RedisSession {
        Socket socket;

        InputStream input;
        OutputStream output;

        public boolean isConnected() {
            return socket.isConnected();
        }
    }

}
