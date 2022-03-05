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
import lombok.RequiredArgsConstructor;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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

    @NonFinal
    int state;

    /**
     * Конструктор редис клиента.
     *
     * @param config Конфиг редис клиента
     */
    public Redis(final Config config) {
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

    private void _resetState() {
        state = STATE_UNKNOWN;
    }

    private void _connect() throws SocketException, AuthException {
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

                        throw new AuthException(nextString());
                    }
                }
            } catch (final IOException e) {
                throw new SocketException("Can't connect to " + address, e);
            }
        }
    }

    /**
     * Подключиться к Redis серверу.
     * <p>
     * Подключение происходит автоматически при {@link #flush()} и {@link #read()},
     * но вы можете подключиться заранее. Например, чтобы проверить, есть ли доступ к
     * Redis серверу.
     *
     * @throws SocketException Выбрасывается, если доступа к Redis серверу нет
     * @throws AuthException   Выбрасывается, если не удалось авторизоваться с Redis сервером
     */
    public void connect() throws SocketException, AuthException {
        _connect();
    }

    /**
     * Записать 32-битное число в буффер записи.
     * <p>
     * Это не спровоцирует подключение к Redis серверу, чтобы отправить данные,
     * воспользуйтесь методом {@link #flush()} или {@link #flushAndRead()}
     *
     * @param number Число
     * @return Текущий экземпляр редис клиента, проще - {@code this}
     */
    public Redis writeInt(final int number) {
        write.writeInt(number);

        return this;
    }

    /**
     * Записать 64-битное число в буффер записи.
     * <p>
     * Это не спровоцирует подключение к Redis серверу, чтобы отправить данные,
     * воспользуйтесь методом {@link #flush()} или {@link #flushAndRead()}
     *
     * @param number Число
     * @return Текущий экземпляр редис клиента, проще - {@code this}
     */
    public Redis writeLong(final long number) {
        write.writeLong(number);

        return this;
    }

    /**
     * Записать {@code UTF} в буффер записи.
     * <p>
     * Этот метод получает из строки байты через метод {@link String#getBytes(Charset)} и записывает
     * их в буффер записи.
     * <p>
     * Если вы уверены в том, что в строке используются только символы из {@code US_ASCII}, вы
     * можете воспользоваться методом {@link #writeAscii(String)}, поскольку он не создаёт временный массив байтов.
     *
     * @param text Текст
     * @return Текущий экземпляр редис клиента, проще - {@code this}
     */
    public Redis writeUTF(final String text) {
        write.writeUTF(text);

        return this;
    }

    /**
     * Записать {@code ASCII} в буффер записи.
     * <p>
     * Этот метод получает массив байтов из строки через {@link VarHandle}, что не создаёт временный массив байтов.
     * <p>
     * Если в вашей строке содержатся символы, которых нет в {@code US_ASCII}, например русский текст и т.д, то следует
     * воспользоваться методом {@link #writeUTF(String)}.
     *
     * @param text Текст
     * @return Текущий экземпляр редис клиента, проще - {@code this}
     */
    public Redis writeAscii(final String text) {
        write.writeAscii(text);

        return this;
    }

    /**
     * Записать байты в буффер записи.
     *
     * @param bytes Массив байтов
     * @return Текущий экземпляр редис клиента, проще - {@code this}
     */
    public Redis writeBytes(final byte[] bytes) {
        write.writeBytes(bytes);

        return this;
    }

    /**
     * Записать команду в буффер записи.
     *
     * @param name      Название команды
     * @param arguments Количество аргументов команды, которые будут записаны далее
     * @return Текущий экземпляр редис клиента, проще - {@code this}
     */
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

    /**
     * Отправить буффер записи на Redis сервер.
     * <p>
     * Если подключение не открыто, это откроет новое подключение.
     */
    @SneakyThrows
    public void flush() {
        _connect();
        _flush();
    }

    /**
     * Отправить буффер записи на Redis сервер и прочитать ответ в буффер чтения.
     * <p>
     * Если подключение не открыто, это откроет новое подключение.
     * <p>
     * Буффером чтения вы можете воспользоваться при помощи методов {@link #nextInt()}, {@link #nextLong()},
     * {@link #nextArray()}, {@link #nextString()} и т.д.
     */
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

    /**
     * Прочитать ответ Redis сервера в буффер чтения.
     * <p>
     * Если подключение не открыто, это откроет новое подключение.
     * <p>
     * Буффером чтения вы можете воспользоваться при помощи методов {@link #nextInt()}, {@link #nextLong()},
     * {@link #nextArray()}, {@link #nextString()} и т.д.
     */
    @SneakyThrows
    public void read() {
        _connect();
        _read();
    }

    /**
     * Закрыть Redis клиент.
     * <p>
     * Это полностью закроет Redis клиент без возможности открыть подключение заново.
     */
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

    private static int digit(final char value) {
        return value >= '0' && value <= '9' ? value & 0xF : -1;
    }

    @SneakyThrows
    private int _readState() {
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
        return "Redis[address=" + address + "]";
    }

    public boolean isError() {
        return _readState() == STATE_ERR;
    }

    /**
     * Прочитать из буффера чтения массив.
     * <p>
     * Вы получаете размер массива, далее в цикле вы можете сделать
     * {@link #nextString()}, {@link #nextInt()} и т.д
     *
     * @return Размер массива.
     */
    public int nextArray() {
        val state = _readState();

        if (state != STATE_ARRAY) {
            throw new IllegalStateException("Cannot read array at " + getStateName(state));
        }

        _resetState();

        return _readInt();
    }

    /**
     * Прочитать строку из буффера чтения.
     *
     * @return Строка
     */
    @SneakyThrows
    public String nextString() {
        val state = _readState();

        val buffer = this.read;

        try {
            if (state == STATE_STRING) {
                val number = _readInt();
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
                _skipUntilCrlf();

                val end = buffer.getPosition() - 2;

                return new String(buffer.getArray(), start, end - start);
            }
        } finally {
            _resetState();
        }
    }

    @SneakyThrows
    private long _readLong() {
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
    private int _readInt() {
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
    private void _skipUntilCrlf() {
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

    /**
     * Пропустить следующие {@code count} элементов из буффера чтения.
     *
     * @param count Количество элементов, которые нужно пропустить
     */
    public void skip(final int count) {
        for (int i = 0; i < count; i++) {
            skip();
        }
    }

    /**
     * Пропустить следующий элемент из буффера чтения.
     */
    public void skip() {
        val state = _readState();

        _skipUntilCrlf();

        // prefixed with number
        if (state == STATE_STRING || state == STATE_ARRAY) {
            _skipUntilCrlf();
        }

        _resetState();
    }

    /**
     * Прочитать массив байт из буффера чтения.
     *
     * @return Массив байт
     */
    public byte[] nextBytes() {
        _readState();

        final RedisBuffer buffer;

        val start = (buffer = this.read).getPosition();
        _skipUntilCrlf();

        val end = buffer.getPosition() - 2;
        _resetState();

        return Arrays.copyOfRange(buffer.getArray(), start, end);
    }

    /**
     * Прочитать массив байт из буффера чтения.
     *
     * @param bytes Вывод
     * @param off   Сдвиг вывода
     * @param len   Размер вывода
     * @return Байт прочитано
     */
    @SneakyThrows
    public int nextBytes(final byte[] bytes, final int off, final int len) {
        _readState();

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
                _resetState();

                break;
            }

            prev = value;
            read++;
        }

        System.arraycopy(buffer.getArray(), start, bytes, off, read);

        return read;
    }

    /**
     * Прочитать массив байт из буффера чтения.
     *
     * @param bytes Вывод
     * @return Байт прочитано
     */
    public int nextBytes(final byte[] bytes) {
        return nextBytes(bytes, 0, bytes.length);
    }

    /**
     * Прочитать 32-битное число из буффера чтения.
     *
     * @return число
     */
    public int nextInt() {
        val state = _readState();

        if (state != STATE_NUMBER) {
            throw new IllegalStateException("Cannot read number at " + getStateName(state));
        }

        _resetState();

        return _readInt();
    }

    /**
     * Прочитать 64-битное число из буффера чтения.
     *
     * @return число
     */
    public long nextLong() {
        val state = _readState();

        if (state != STATE_NUMBER) {
            throw new IllegalStateException("Cannot read number at " + getStateName(state));
        }

        _resetState();

        return _readLong();
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

    @Getter
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @RequiredArgsConstructor
    public static final class Config {
        InetSocketAddress address;
        int writeBufferCapacity;
        int readBufferCapacity;
        int soSndBuf;
        int soRcvBuf;
        long connectTimeoutMillis;
        boolean tcpNoDelay;
        String username;
        String password;

        @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
        @RequiredArgsConstructor
        public static final class Builder {

            InetSocketAddress address;

            @NonFinal
            String username;

            @NonFinal
            String password;

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

            public Builder auth(
                    final String username,
                    final String password
            ) {
                this.username = username;
                this.password = password;

                return this;
            }

            public Builder auth(final String password) {
                this.username = null;
                this.password = password;

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
            public Builder soRcvBuf(final int value) {
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
            public Builder soSndBuf(final int value) {
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
            public Builder writeBufferCapacity(final int capacity) {
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
            public Builder readBufferCapacity(final int capacity) {
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
            public Builder connectTimeout(final long timeout, final TimeUnit timeUnit) {
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
            public Builder tcpNoDelay(final boolean tcpNoDelay) {
                this.tcpNoDelay = tcpNoDelay;

                return this;
            }

            public Config build() {
                return new Config(
                        address,
                        writeCapacity == null ? 1024 : writeCapacity,
                        readCapacity == null ? 1024 : readCapacity,
                        soSndBuf == null ? 1024 : soSndBuf,
                        soRcvBuf == null ? 1024 : soRcvBuf,
                        timeout,
                        tcpNoDelay,
                        username,
                        password
                );
            }
        }
    }

    public static abstract class RedisException extends RuntimeException {
        protected RedisException(final String message) {
            super(message);
        }

        protected RedisException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    public static final class SocketException extends RedisException {
        public SocketException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    public static final class AuthException extends RedisException {
        public AuthException(final String message) {
            super(message);
        }
    }

}
