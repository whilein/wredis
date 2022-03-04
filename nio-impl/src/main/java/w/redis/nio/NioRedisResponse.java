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
import w.redis.Redis;
import w.redis.RedisResponse;
import w.redis.buffer.ReadRedisBuffer;

import java.lang.reflect.Modifier;
import java.util.Arrays;

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

    // for error logging purposes
    @SneakyThrows
    private static String getStateName(final int state) {
        for (val field : NioRedisResponse.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                if (state == field.getInt(null)) {
                    return field.getName();
                }
            }
        }

        return "";
    }

    Redis redis;

    ReadRedisBuffer buffer;

    @NonFinal
    int state;

    public static @NotNull RedisResponse create(
            final @NotNull Redis redis,
            final @NotNull ReadRedisBuffer redisBuffer
    ) {
        return new NioRedisResponse(redis, redisBuffer);
    }

    @Override
    public void resetState() {
        state = STATE_UNKNOWN;
    }

    private int digit(final char value) {
        return value >= '0' && value <= '9' ? value & 0xF : -1;
    }

    private int readState() {
        if (state == STATE_UNKNOWN) {
            if (!buffer.hasRemaining()) {
                redis.readMore();
            }

            val value = buffer.getNext();

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
                default:
                    throw new IllegalArgumentException("Illegal token: " + (char) value + "(bin: " + value + ")");
            }
        }

        return state;
    }

    private void ensureReadable(final int amount) {
        val remaining = buffer.remaining();

        if (amount > remaining) {
            redis.readMore();
        }
    }

    @Override
    public String toString() {
        return "\"" + new String(buffer.getArray(), 0, buffer.getLength())
                .replace("\r", "\\r")
                .replace("\n", "\\n") + "\"";
    }

    @Override
    public boolean isError() {
        return readState() == STATE_ERR;
    }

    @Override
    public int nextArray() {
        val state = readState();

        if (state != STATE_ARRAY) {
            throw new IllegalStateException("Cannot read array at " + getStateName(state));
        }

        resetState();

        return readInt();
    }

    @Override
    public @NotNull String nextString() {
        val state = readState();

        try {
            if (state == STATE_STRING) {
                val number = readInt();
                val offset = buffer.getPosition();

                ensureReadable(number + 2); // string length + crlf

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

    private long readLong() {
        byte prev = 0, value;

        boolean negative = false;
        long result = 0;

        while (true) {
            while (buffer.hasRemaining()) {
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

            redis.readMore();
        }
    }

    private int readInt() {
        byte prev = 0, value;

        boolean negative = false;
        int result = 0;

        while (true) {
            while (buffer.hasRemaining()) {
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

            redis.readMore();
        }
    }

    private void skipUntilCrlf() {
        byte prev = 0, value;

        while (true) {
            while (buffer.hasRemaining()) {
                value = buffer.getNext();

                if (value == '\n' && prev == '\r') {
                    return;
                }

                prev = value;
            }

            redis.readMore();
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
        val state = readState();

        skipUntilCrlf();

        // prefixed with number
        if (state == STATE_STRING || state == STATE_ARRAY) {
            skipUntilCrlf();
        }

        resetState();
    }

    @Override
    public byte @NotNull [] nextBytes() {
        readState();

        val start = buffer.getPosition();
        skipUntilCrlf();

        val end = buffer.getPosition() - 2;
        resetState();

        return Arrays.copyOfRange(buffer.getArray(), start, end);
    }

    @Override
    public int nextBytes(final byte @NotNull [] bytes, final int off, final int len) {
        readState();

        val start = buffer.getPosition();

        byte prev = 0, value;
        int read = 0;

        root:
        while (true) {
            while (buffer.hasRemaining()) {
                if (read >= len) {
                    break root;
                }

                value = buffer.getNext();

                if (value == '\n' && prev == '\r') {
                    read--; // remove \r from length
                    resetState();

                    break root;
                }

                prev = value;
                read++;
            }

            redis.readMore();
        }

        System.arraycopy(buffer.getArray(), start, bytes, off, read);

        return read;
    }

    @Override
    public int nextBytes(final byte @NotNull [] bytes) {
        return nextBytes(bytes, 0, bytes.length);
    }

    @Override
    public int nextInt() {
        val state = readState();

        if (state != STATE_NUMBER) {
            throw new IllegalStateException("Cannot read number at " + getStateName(state));
        }

        resetState();

        return readInt();
    }

    @Override
    public long nextLong() {
        val state = readState();

        if (state != STATE_NUMBER) {
            throw new IllegalStateException("Cannot read number at " + getStateName(state));
        }

        resetState();

        return readLong();
    }

}
