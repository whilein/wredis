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

package w.redis.buffer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.FieldDefaults;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import w.redis.AsciiWriter;
import w.redis.util.NumberUtils;

import java.nio.charset.StandardCharsets;

/**
 * @author whilein
 */
@UtilityClass
public class RedisBuffers {

    public static @NotNull ReadRedisBuffer readBuffer(final int capacity) {
        return new ReadRedisBufferImpl(new byte[capacity], 0);
    }

    public static @NotNull WriteRedisBuffer writeBuffer(final int capacity, final @NotNull AsciiWriter asciiWriter) {
        return new WriteRedisBufferImpl(new byte[capacity], 0, asciiWriter);
    }

    private static final class WriteRedisBufferImpl extends AbstractRedisBuffer implements WriteRedisBuffer {

        AsciiWriter asciiWriter;

        public WriteRedisBufferImpl(final byte[] array, final int position, final AsciiWriter asciiWriter) {
            super(array, position);

            this.asciiWriter = asciiWriter;
        }

        @Override
        public void ensure(final int len) {
            final int requiredCapacity = position + len;
            final int currentCapacity = getCapacity();

            if (requiredCapacity > currentCapacity) {
                resize(Math.max(requiredCapacity, currentCapacity * 2));
            }
        }

        @Override
        public void writeRaw(final byte value) {
            array[position++] = value;
        }

        @Override
        public void writeRaw(final byte @NotNull [] value) {
            val length = value.length;
            System.arraycopy(value, 0, array, position, length);
            this.position += length;
        }

        public void writeCrlf() {
            writeRaw((byte) '\r');
            writeRaw((byte) '\n');
        }

        @Override
        public void writeCommand(final @NotNull String command, final int arguments) {
            writeLength('*', arguments + 1);
            val commandLength = command.length();
            writeLength('$', commandLength);

            ensure(commandLength + 2);
            _writeAscii(command);
            writeCrlf();
        }

        public void _writeAscii(final @NotNull String ascii) {
            asciiWriter.write(ascii, this);
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

        private void writeLength(final char prefix, final int length) {
            if (length < 10) {
                ensure(4);
                writeRaw((byte) prefix);
                writeRaw((byte) ('0' + length));
                writeCrlf();
                return;
            }

            val lengthOfNumber = NumberUtils.getIntLength(length);

            // 3 байта на префикс и crlf, остальное на число
            ensure(3 + lengthOfNumber);
            writeRaw((byte) prefix);

            val position = this.position + lengthOfNumber;
            _writeInt(position, length);
            writeCrlf();
        }

        @Override
        public void writeInt(final int rawNumber) {
            final int number;
            final int length;

            final boolean negative;

            if ((negative = rawNumber < 0)) {
                length = NumberUtils.getIntLength(number = -rawNumber) + 1;
            } else {
                length = NumberUtils.getIntLength(number = rawNumber);
            }

            writeLength('$', length);

            ensure(length + 2);

            val position = this.position + length;
            val lastPosition = _writeInt(position, number);

            if (negative) {
                array[lastPosition - 1] = (byte) '-';
            }

            writeCrlf();
        }

        @Override
        public void writeLong(final long rawNumber) {
            final long number;
            final int length;

            final boolean negative;

            if ((negative = rawNumber < 0)) {
                length = NumberUtils.getLongLength(number = -rawNumber) + 1;
            } else {
                length = NumberUtils.getLongLength(number = rawNumber);
            }

            writeLength('$', length);

            ensure(length + 2);

            val position = this.position + length;
            val lastPosition = _writeLong(position, number);

            if (negative) {
                array[lastPosition - 1] = (byte) '-';
            }

            writeCrlf();
        }

        @Override
        public void writeEmptyString() {
            ensure(4);

            writeRaw((byte) '$');
            writeRaw((byte) '0');
            writeCrlf();
        }

        @Override
        public void writeBytes(final byte[] bytes) {
            val blobLength = bytes.length;

            if (blobLength == 0) {
                writeEmptyString();
                return;
            }

            writeLength('$', blobLength);

            ensure(blobLength + 2);
            writeRaw(bytes);
            writeCrlf();
        }

        @Override
        public void writeAscii(final @NotNull String text) {
            if (text.length() == 0) {
                writeEmptyString();
                return;
            }

            val textLength = text.length();
            writeLength('$', textLength);

            ensure(textLength + 2);
            _writeAscii(text);

            writeCrlf();
        }

        @Override
        public void writeUTF(final @NotNull String text) {
            if (text.length() == 0) {
                writeEmptyString();
                return;
            }

            val bytes = text.getBytes(StandardCharsets.UTF_8);

            val textLength = bytes.length;
            writeLength('$', textLength);

            ensure(textLength + 2);
            writeRaw(bytes);

            writeCrlf();
        }
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    private static final class ReadRedisBufferImpl extends AbstractRedisBuffer implements ReadRedisBuffer {

        private ReadRedisBufferImpl(final byte[] array, final int position) {
            super(array, position);
        }

        @Getter
        @Setter
        int length;

        @Override
        public byte getNext() {
            return array[position++];
        }

        @Override
        public int remaining() {
            return length - position;
        }

        @Override
        public boolean hasRemaining() {
            return position != length;
        }
    }

}
