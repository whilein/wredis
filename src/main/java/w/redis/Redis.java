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
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
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
public final class Redis implements AutoCloseable {

    InetSocketAddress address;

    String username;
    String password;

    int soSndBuf;
    int soRcvBuf;

    WriteRedisBuffer write;

    ReadRedisBuffer read;

    RedisResponse response;

    long timeout;

    boolean tcpNoDelay;

    @NonFinal
    boolean closed;

    @NonFinal
    Socket socket;

    @NonFinal
    InputStream input;

    @NonFinal
    OutputStream output;

    public Redis(final RedisConfig config) {
        this.address = config.getAddress();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.soSndBuf = config.getSoSndBuf();
        this.soRcvBuf = config.getSoRcvBuf();
        this.write = new WriteRedisBuffer(new byte[config.getWriteBufferCapacity()], 0);
        this.read = new ReadRedisBuffer(new byte[config.getReadBufferCapacity()], 0);
        this.timeout = config.getConnectTimeoutMillis();
        this.tcpNoDelay = config.isTcpNoDelay();

        this.response = new RedisResponse(this, read);
    }

    private void _connect() throws RedisSocketException {
        if (socket == null || !socket.isConnected()) {
            if (closed) {
                throw new IllegalStateException("Redis instance was closed");
            }

            try {
                socket = new Socket();
                socket.setTcpNoDelay(tcpNoDelay);
                socket.setSendBufferSize(soSndBuf);
                socket.setReceiveBufferSize(soRcvBuf);
                socket.connect(address, (int) timeout);

                output = socket.getOutputStream();
                input = socket.getInputStream();

                if (password != null) {
                    // Может произойти такая ситуация, что кто-то уже записал что-то в буффер до коннекта
                    // поэтому нужно создать отдельный буффер..

                    val authBuffer = new WriteRedisBuffer(new byte[64], 0);

                    if (username != null) {
                        authBuffer.writeCommand("AUTH", 2);
                        authBuffer.writeUTF(username);
                        authBuffer.writeUTF(password);
                    } else {
                        authBuffer.writeCommand("AUTH", 1);
                        authBuffer.writeUTF(password);
                    }

                    _flush(authBuffer);
                    _read();

                    final RedisResponse response;

                    if ((response = this.response).isError()) {
                        socket = null;
                        output = null;
                        input = null;

                        throw new RedisAuthException(response.nextString());
                    }
                }
            } catch (final IOException e) {
                throw new RedisSocketException("Can't connect to " + address, e);
            }
        }
    }

    public Redis connect() throws RedisSocketException {
        _connect();

        return this;
    }

    public Redis writeInt(final int number) {
        write.writeInt(number);

        return this;
    }

    public Redis writeLong(final long number) {
        write.writeLong(number);

        return this;
    }

    public Redis writeUTF(final String text) {
        write.writeUTF(text);

        return this;
    }

    public Redis writeAscii(final String text) {
        write.writeAscii(text);

        return this;
    }

    public Redis writeBytes(final byte[] bytes) {
        write.writeBytes(bytes);

        return this;
    }

    public Redis writeCommand(final String name, final int arguments) {
        write.writeCommand(name, arguments);

        return this;
    }

    private void _flush() throws IOException {
        _flush(write);
    }

    private void _flush(final WriteRedisBuffer buffer) throws IOException {
        output.write(buffer.getArray(), 0, buffer.getPosition());
        buffer.setPosition(0);
    }

    @SneakyThrows
    public void flush() {
        _connect();
        _flush();
    }

    @SneakyThrows
    public RedisResponse flushAndRead() {
        _connect();

        _flush();
        _read();

        return response;
    }

    private void _read() throws IOException {
        val readBuffer = read;
        readBuffer.setPosition(0);
        readBuffer.setLength(0);

        val array = readBuffer.getArray();

        final int read;

        if ((read = input.read(array, 0, array.length)) == readBuffer.getCapacity()) {
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
    public RedisResponse read() {
        _connect();
        _read();

        return response;
    }

    @Override
    @SneakyThrows
    public void close() {
        if (socket != null) {
            closed = true;

            socket.close();
            socket = null;

            output = null;
            input = null;
        }
    }

}
