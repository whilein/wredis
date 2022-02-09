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
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import w.redis.RedisResponse;

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.IntConsumer;

/**
 * @author whilein
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NioRedisResponse implements RedisResponse {

    private static final int STATE_ARRAY = 5;
    private static final int STATE_NUMBER = 4;
    private static final int STATE_STRING = 3;
    private static final int STATE_OK = 2;
    private static final int STATE_ERR = 1;
    private static final int STATE_UNKNOWN = 0;
    private static final int STATE_EOF = -1;

    // for error logging purposes
    @SneakyThrows
    private static String getStateName(final int state) {
        for (val field : NioRedisResponse.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                if(state == field.getInt(null)) {
                    return field.getName();
                }
            }
        }

        return "";
    }

    public static @NotNull RedisResponse create(final @NotNull ByteBuffer buffer) {
        return new NioRedisResponse(buffer);
    }

    ByteBuffer buffer;

    @NonFinal
    int state;

    private void resetState() {
        state = STATE_UNKNOWN;
    }

    private int digit(final char value) {
        return value >= '0' && value <= '9' ? value & 0xF : -1;
    }

    private int readState() {
        if (state == STATE_UNKNOWN) {
            if (!buffer.hasRemaining()) {
                return state = STATE_EOF;
            }

            val value = buffer.get();

            switch (value) {
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
            }
        }

        return state;
    }

    private int ensureNext() {
        final int state = readState();

        if (state == STATE_EOF) {
            throw new IllegalStateException("No more data in response");
        }

        return state;
    }

    @Override
    public boolean hasNext() {
        return readState() != STATE_EOF;
    }

    @Override
    public boolean isError() {
        return readState() == STATE_ERR;
    }

    @Override
    public int nextArray() {
        val state = ensureNext();

        if (state != STATE_ARRAY) {
            throw new IllegalStateException("Cannot read array at " + getStateName(state));
        }

        resetState();

        return readInt();
    }

    @Override
    public @NotNull String nextString() {
        val state = ensureNext();

        try {
            if (state == STATE_STRING) {
                val number = readInt();
                val offset = buffer.position();

                try {
                    return new String(buffer.array(), offset, number);
                } finally {
                    buffer.position(offset + number + 2); // skip string with crlf
                }
            } else {
                val start = buffer.position();
                skipUntilCrlf();

                val end = buffer.position() - 2;

                return new String(buffer.array(), start, end - start);
            }
        } finally {
            resetState();
        }
    }

    private boolean readNumber(final IntConsumer digitConsumer) {
        byte prev = 0, value;

        boolean negative = false;

        while (true) {
            value = buffer.get();

            if (prev == 0 && value == '-') {
                negative = true;
            } else if (value == '\n' && prev == '\r') {
                break;
            }

            prev = value;

            val digit = digit((char) value);

            if (digit != -1) {
                digitConsumer.accept(digit);
            }
        }

        return negative;
    }

    @NonFinal
    long readLongValue;

    @NonFinal
    int readIntValue;

    private long readLong() {
        readLongValue = 0;

        val negative = readNumber(digit -> readLongValue = readLongValue * 10L + digit);
        return negative ? -readLongValue : readLongValue;
    }

    private int readInt() {
        readIntValue = 0;

        val negative = readNumber(digit -> readIntValue = readIntValue * 10 + digit);
        return negative ? -readIntValue : readIntValue;
    }

    private void skipUntilCrlf() {
        byte prev = 0, value;

        while (true) {
            value = buffer.get();

            if (value == '\n' && prev == '\r') {
                break;
            }

            prev = value;
        }
    }

    @Override
    public void skip(final int count) {
        for (int i = 0; i < count; i++) {
            skip();
        }
    }

    @Override
    public void skip() {
        val state = ensureNext();

        skipUntilCrlf();

        // prefixed with number
        if (state == STATE_STRING || state == STATE_ARRAY) {
            skipUntilCrlf();
        }

        resetState();
    }

    @Override
    public byte @NotNull [] nextBytes() {
        ensureNext();

        val start = buffer.position();
        skipUntilCrlf();

        val end = buffer.position() - 2;
        resetState();

        return Arrays.copyOfRange(buffer.array(), start, end);
    }

    @Override
    public int nextBytes(final byte @NotNull [] bytes, final int off, final int len) {
        ensureNext();

        val start = buffer.position();

        byte prev = 0, value;
        int read = 0;

        while (true) {
            if (read >= len) {
                break;
            }

            value = buffer.get();

            if (value == '\n' && prev == '\r') {
                read--; // remove \r from length
                resetState();
                break;
            }

            prev = value;
            read++;
        }

        System.arraycopy(buffer.array(), start, bytes, off, read);

        return read;
    }

    @Override
    public int nextBytes(final byte @NotNull [] bytes) {
        return nextBytes(bytes, 0, bytes.length);
    }

    @Override
    public int nextInt() {
        val state = ensureNext();

        if (state != STATE_NUMBER) {
            throw new IllegalStateException("Cannot read number at " + getStateName(state));
        }

        resetState();

        return readInt();
    }

    @Override
    public long nextLong() {
        val state = ensureNext();

        if (state != STATE_NUMBER) {
            throw new IllegalStateException("Cannot read number at " + getStateName(state));
        }

        resetState();

        return readLong();
    }

}
