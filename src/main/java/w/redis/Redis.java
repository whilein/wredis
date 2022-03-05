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
import w.redis.buffer.RedisBuffer;
import w.redis.buffer.WriteRedisBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

/**
 * @author whilein
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public final class Redis implements AutoCloseable {

    private static final int STATE_ARRAY = 5;
    private static final int STATE_NUMBER = 4;
    private static final int STATE_STRING = 3;
    private static final int STATE_OK = 2;
    private static final int STATE_ERR = 1;
    private static final int STATE_UNKNOWN = 0;

    // for error logging purposes
    @SneakyThrows
    private static String getStateName(final int state) {
        for (val field : Redis.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getName().startsWith("STATE_")) {
                if (state == field.getInt(null)) {
                    return field.getName();
                }
            }
        }

        return "";
    }

    InetSocketAddress address;

    String username;
    String password;

    int soSndBuf;
    int soRcvBuf;

    WriteRedisBuffer write;

    ReadRedisBuffer read;

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

                    final WriteRedisBuffer authBuffer;

                    if (username != null) {
                        // магические цифры.. я просто чекнул position и вычел username.length и password.length =)
                        // а ещё будем надеяться, что username и password в US_ASCII
                        authBuffer = new WriteRedisBuffer(new byte[27 + username.length() + password.length()],
                                0);
                        authBuffer.writeCommand("AUTH", 2);

                        // я бы сделал ascii, но кто знает, мб у людей юзер/пароль на русском
                        authBuffer.writeUTF(username);
                        authBuffer.writeUTF(password);
                    } else {
                        authBuffer = new WriteRedisBuffer(new byte[21 + password.length()], 0);
                        authBuffer.writeCommand("AUTH", 1);
                        authBuffer.writeUTF(password);
                    }

                    _flush(authBuffer);
                    _read();

                    if (isError()) {
                        socket = null;
                        output = null;
                        input = null;

                        throw new RedisAuthException(nextString());
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
    public void flushAndRead() {
        _connect();

        _flush();
        _read();
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
        state = STATE_UNKNOWN;
    }

    @SneakyThrows
    public void read() {
        _connect();
        _read();
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

    @NonFinal
    int state;

    public void resetState() {
        state = STATE_UNKNOWN;
    }

    private static int digit(final char value) {
        return value >= '0' && value <= '9' ? value & 0xF : -1;
    }

    @SneakyThrows
    private int readState() {
        if (state == STATE_UNKNOWN) {
            final ReadRedisBuffer buffer;

            if (!(buffer = this.read).hasRemaining()) {
                _read();
            }

            final byte value;

            switch ((value = buffer.getNext())) {
                case '*':
                    return state = STATE_ARRAY;
                case ':':
                    return state = STATE_NUMBER;
                case '$':
                    return state = STATE_STRING;
                case '+':
                    return state = STATE_OK;
                case '-':
                    return state = STATE_ERR;
                default:
                    throw new IllegalArgumentException("Illegal token: " + (char) value + " (bin: " + value + ")");
            }
        }

        return state;
    }

    @Override
    public String toString() {
        val buffer = this.read;

        return "\"" + new String(buffer.getArray(), 0, buffer.getLength())
                .replace("\r", "\\r")
                .replace("\n", "\\n") + "\"";
    }

    public boolean isError() {
        return readState() == STATE_ERR;
    }

    public int nextArray() {
        val state = readState();

        if (state != STATE_ARRAY) {
            throw new IllegalStateException("Cannot read array at " + getStateName(state));
        }

        resetState();

        return readInt();
    }

    @SneakyThrows
    public String nextString() {
        val state = readState();

        val buffer = this.read;

        try {
            if (state == STATE_STRING) {
                val number = readInt();
                val offset = buffer.getPosition();

                val remaining = buffer.remaining();

                if ((number + 2) > remaining) {
                    _read();
                }

                try {
                    return new String(buffer.getArray(), offset, number);
                } finally {
                    buffer.setPosition(offset + number + 2); // skip string with crlf
                }
            } else {
                val start = buffer.getPosition();
                skipUntilCrlf();

                val end = buffer.getPosition() - 2;

                return new String(buffer.getArray(), start, end - start);
            }
        } finally {
            resetState();
        }
    }

    @SneakyThrows
    private long readLong() {
        byte prev = 0, value;

        boolean negative = false;
        long result = 0;

        val buffer = this.read;

        while (true) {
            if (!buffer.hasRemaining()) {
                _read();
            }

            value = buffer.getNext();

            if (prev == 0 && value == '-') {
                negative = true;
            } else if (value == '\n' && prev == '\r') {
                return negative ? -result : result;
            }

            prev = value;

            val digit = digit((char) value);

            if (digit != -1) {
                result = result * 10 + digit;
            }
        }
    }

    @SneakyThrows
    private int readInt() {
        byte prev = 0, value;

        boolean negative = false;
        int result = 0;

        val buffer = this.read;

        while (true) {
            if (!buffer.hasRemaining()) {
                _read();
            }

            value = buffer.getNext();

            if (prev == 0 && value == '-') {
                negative = true;
            } else if (value == '\n' && prev == '\r') {
                return negative ? -result : result;
            }

            prev = value;

            val digit = digit((char) value);

            if (digit != -1) {
                result = result * 10 + digit;
            }
        }
    }

    @SneakyThrows
    private void skipUntilCrlf() {
        byte prev = 0;

        val buffer = this.read;

        while (true) {
            if (!buffer.hasRemaining()) {
                _read();
            }

            byte value;

            if ((value = buffer.getNext()) == 10 && prev == 13) {
                return;
            }

            prev = value;
        }
    }

    public void skip(final int count) {
        for (int i = 0; i < count; i++) {
            skip();
        }
    }

    public void skip() {
        val state = readState();

        skipUntilCrlf();

        // prefixed with number
        if (state == STATE_STRING || state == STATE_ARRAY) {
            skipUntilCrlf();
        }

        resetState();
    }

    public byte[] nextBytes() {
        readState();

        final RedisBuffer buffer;

        val start = (buffer = this.read).getPosition();
        skipUntilCrlf();

        val end = buffer.getPosition() - 2;
        resetState();

        return Arrays.copyOfRange(buffer.getArray(), start, end);
    }

    @SneakyThrows
    public int nextBytes(final byte[] bytes, final int off, final int len) {
        readState();

        final ReadRedisBuffer buffer;

        val start = (buffer = this.read).getPosition();

        byte prev = 0, value;
        int read = 0;

        while (true) {
            if (!buffer.hasRemaining()) {
                _read();
            }

            if (read >= len) {
                break;
            }

            value = buffer.getNext();

            if (value == '\n' && prev == '\r') {
                read--; // remove \r from length
                resetState();

                break;
            }

            prev = value;
            read++;
        }

        System.arraycopy(buffer.getArray(), start, bytes, off, read);

        return read;
    }

    public int nextBytes(final byte[] bytes) {
        return nextBytes(bytes, 0, bytes.length);
    }

    public int nextInt() {
        val state = readState();

        if (state != STATE_NUMBER) {
            throw new IllegalStateException("Cannot read number at " + getStateName(state));
        }

        resetState();

        return readInt();
    }

    public long nextLong() {
        val state = readState();

        if (state != STATE_NUMBER) {
            throw new IllegalStateException("Cannot read number at " + getStateName(state));
        }

        resetState();

        return readLong();
    }

}
