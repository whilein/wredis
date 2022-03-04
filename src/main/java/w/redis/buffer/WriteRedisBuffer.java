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

import lombok.val;
import w.redis.util.Internals;
import w.redis.util.NumberUtils;

import java.nio.charset.StandardCharsets;

/**
 * @author whilein
 */
public final class WriteRedisBuffer extends RedisBuffer {

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

    public void _writeAscii(final String ascii) {
        val bytes = Internals.getBytes(ascii);
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

        val lengthOfNumber = NumberUtils.getIntLength(length);

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
            length = NumberUtils.getIntLength(number = -rawNumber) + 1;
        } else {
            length = NumberUtils.getIntLength(number = rawNumber);
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
            length = NumberUtils.getLongLength(number = -rawNumber) + 1;
        } else {
            length = NumberUtils.getLongLength(number = rawNumber);
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
