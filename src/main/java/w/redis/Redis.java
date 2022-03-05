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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
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

    private static final VarHandle VH__STRING_VALUE;

    static {
        final MethodHandles.Lookup implLookup;

        // region IMPL_LOOKUP
        try {
            val theUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);

            val theUnsafe = (Unsafe) theUnsafeField.get(null);

            val implLookupField = MethodHandles.Lookup.class.getDeclaredField("IMPL_LOOKUP");

            implLookup = (MethodHandles.Lookup) theUnsafe.getObject(
                    theUnsafe.staticFieldBase(implLookupField),
                    theUnsafe.staticFieldOffset(implLookupField)
            );

            VH__STRING_VALUE = implLookup.findVarHandle(String.class, "value", byte[].class);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

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

    private static int getLongLength(final long value) {
        if (value < 1000000000) {
            if (value < 100000) {
                if (value < 100) {
                    if (value < 10) {
                        return 1;
                    } else {
                        return 2;
                    }
                } else {
                    if (value < 1000) {
                        return 3;
                    } else {
                        if (value < 10000) {
                            return 4;
                        } else {
                            return 5;
                        }
                    }
                }
            } else {
                if (value < 10000000) {
                    if (value < 1000000) {
                        return 6;
                    } else {
                        return 7;
                    }
                } else {
                    if (value < 100000000) {
                        return 8;
                    } else {
                        return 9;
                    }
                }
            }
        } else {
            if (value < 100000000000000L) {
                if (value < 100000000000L) {
                    if (value < 10000000000L) {
                        return 10;
                    } else {
                        return 11;
                    }
                } else {
                    if (value < 1000000000000L) {
                        return 12;
                    } else {
                        if (value < 10000000000000L) {
                            return 13;
                        } else {
                            return 14;
                        }
                    }
                }
            } else {
                if (value < 10000000000000000L) {
                    if (value < 1000000000000000L) {
                        return 15;
                    } else {
                        return 16;
                    }
                } else {
                    if (value < 100000000000000000L) {
                        return 17;
                    } else {
                        if (value < 1000000000000000000L) {
                            return 18;
                        } else {
                            return 19;
                        }
                    }
                }
            }
        }
    }

    private static int getIntLength(final int value) {
        if (value < 100000) {
            if (value < 100) {
                if (value < 10) {
                    return 1;
                } else {
                    return 2;
                }
            } else {
                if (value < 1000) {
                    return 3;
                } else {
                    if (value < 10000) {
                        return 4;
                    } else {
                        return 5;
                    }
                }
            }
        } else {
            if (value < 10000000) {
                if (value < 1000000) {
                    return 6;
                } else {
                    return 7;
                }
            } else {
                if (value < 100000000) {
                    return 8;
                } else {
                    if (value < 1000000000) {
                        return 9;
                    } else {
                        return 10;
                    }
                }
            }
        }
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

    @Getter
    @FieldDefaults(level = AccessLevel.PROTECTED)
    @AllArgsConstructor(access = AccessLevel.PROTECTED)
    private static abstract class RedisBuffer {

        byte[] array;

        @Setter
        int position;

        public int getCapacity() {
            return array.length;
        }

        public void resize() {
            resize(array.length * 2);
        }

        public void resize(final int to) {
            array = Arrays.copyOf(array, to);
        }
    }

    private static final class ReadRedisBuffer extends RedisBuffer {

        public ReadRedisBuffer(final byte[] array, final int position) {
            super(array, position);
        }

        @Getter
        @Setter
        int length;

        public byte getNext() {
            return array[position++];
        }

        public int remaining() {
            return length - position;
        }

        public boolean hasRemaining() {
            return position != length;
        }

    }

    private static final class WriteRedisBuffer extends RedisBuffer {

        public WriteRedisBuffer(final byte[] array, final int position) {
            super(array, position);
        }

        private void _ensure(final int len) {
            final int requiredCapacity = position + len;
            final int currentCapacity = getCapacity();

            if (requiredCapacity > currentCapacity) {
                resize(Math.max(requiredCapacity, currentCapacity * 2));
            }
        }

        public void writeRaw(final byte value) {
            array[position++] = value;
        }

        public void writeRaw(final byte[] value) {
            val length = value.length;
            System.arraycopy(value, 0, array, position, length);
            this.position += length;
        }

        private void _writeCrlf() {
            writeRaw((byte) '\r');
            writeRaw((byte) '\n');
        }

        public void writeCommand(final String command, final int arguments) {
            _writeLength('*', arguments + 1);
            val commandLength = command.length();
            _writeLength('$', commandLength);

            _ensure(commandLength + 2);
            _writeAscii(command);
            _writeCrlf();
        }

        private void _writeAscii(final String ascii) {
            val bytes = (byte[]) VH__STRING_VALUE.get(ascii);

            _ensure(bytes.length);
            writeRaw(bytes);
        }

        private int _writeLong(int position, long value) {
            this.position = position;

            while (value > 0) {
                array[--position] = (byte) ((byte) (value % 10) + '0');
                value /= 10;
            }

            return position;
        }


        private int _writeInt(int position, int value) {
            this.position = position;

            while (value > 0) {
                array[--position] = (byte) ((byte) (value % 10) + '0');
                value /= 10;
            }

            return position;
        }

        private void _writeLength(final char prefix, final int length) {
            if (length < 10) {
                _ensure(4);
                writeRaw((byte) prefix);
                writeRaw((byte) ('0' + length));
                _writeCrlf();
                return;
            }

            val lengthOfNumber = getIntLength(length);

            // 3 байта на префикс и crlf, остальное на число
            _ensure(3 + lengthOfNumber);
            writeRaw((byte) prefix);

            val position = this.position + lengthOfNumber;
            _writeInt(position, length);
            _writeCrlf();
        }

        public void writeInt(final int rawNumber) {
            final int number;
            final int length;

            final boolean negative;

            if ((negative = rawNumber < 0)) {
                length = getIntLength(number = -rawNumber) + 1;
            } else {
                length = getIntLength(number = rawNumber);
            }

            _writeLength('$', length);

            _ensure(length + 2);

            val position = this.position + length;
            val lastPosition = _writeInt(position, number);

            if (negative) {
                array[lastPosition - 1] = (byte) '-';
            }

            _writeCrlf();
        }

        public void writeLong(final long rawNumber) {
            final long number;
            final int length;

            final boolean negative;

            if ((negative = rawNumber < 0)) {
                length = getLongLength(number = -rawNumber) + 1;
            } else {
                length = getLongLength(number = rawNumber);
            }

            _writeLength('$', length);

            _ensure(length + 2);

            val position = this.position + length;
            val lastPosition = _writeLong(position, number);

            if (negative) {
                array[lastPosition - 1] = (byte) '-';
            }

            _writeCrlf();
        }

        private void _writeEmptyString() {
            _ensure(4);

            writeRaw((byte) '$');
            writeRaw((byte) '0');
            _writeCrlf();
        }

        public void writeBytes(final byte[] bytes) {
            val blobLength = bytes.length;

            if (blobLength == 0) {
                _writeEmptyString();
                return;
            }

            _writeLength('$', blobLength);

            _ensure(blobLength + 2);
            writeRaw(bytes);
            _writeCrlf();
        }

        public void writeAscii(final String text) {
            if (text.length() == 0) {
                _writeEmptyString();
                return;
            }

            val textLength = text.length();
            _writeLength('$', textLength);

            _ensure(textLength + 2);
            _writeAscii(text);

            _writeCrlf();
        }

        public void writeUTF(final String text) {
            if (text.length() == 0) {
                _writeEmptyString();
                return;
            }

            val bytes = text.getBytes(StandardCharsets.UTF_8);

            val textLength = bytes.length;
            _writeLength('$', textLength);

            _ensure(textLength + 2);
            writeRaw(bytes);

            _writeCrlf();
        }
    }

}
