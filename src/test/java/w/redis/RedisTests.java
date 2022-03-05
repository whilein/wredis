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

import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * @author whilein
 */
final class RedisTests {

    static Redis redis;

    @BeforeAll
    static void setup() {
        boolean connected;

        try {
            redis = new Redis(new Redis.Config.Builder(new InetSocketAddress("localhost", 6379))
                    .connectTimeout(1, TimeUnit.SECONDS)
                    .auth("default", "1234567890")
                    .build());

            // чтобы проверить, запущен ли редис сервер или нет
            redis.connect();

            connected = true;
        } catch (final Exception e) {
            connected = false;
        }

        assumeTrue(connected);
    }

    @Test
    void readString() {
        redis.writeCommand("PING", 0).flushAndRead();

        assertEquals("PONG", redis.nextString());
    }

    @Test
    void bufferOverflow() {
        for (int i = 0; i < 1000; i++) {
            redis.writeCommand("SISMEMBER", 2)
                    .writeAscii("ABCDEF1234567890")
                    .writeBytes("ABCDEF1234567890".getBytes(StandardCharsets.UTF_8));
        }

        redis.flushAndRead();

        for (int i = 0; i < 1000; i++) {
            assertEquals(0, redis.nextInt());
        }
    }

    @Test
    void longArgument() {
        redis
                .writeCommand("SET", 2)
                .writeAscii("COUNTER")
                .writeAscii("15")

                .writeCommand("DECRBY", 2)
                .writeAscii("COUNTER")
                .writeLong(10L)

                .writeCommand("DECRBY", 2)
                .writeAscii("COUNTER")
                .writeLong(-10L)

                .writeCommand("DEL", 1)
                .writeAscii("COUNTER")

                .flushAndRead();

        assertEquals("OK", redis.nextString()); // SET
        assertEquals(5, redis.nextLong()); // DECRBY
        assertEquals(15, redis.nextLong()); // DECRBY
        assertEquals(1, redis.nextInt()); // DEL
    }

    @Test
    void intArgument() {
        redis
                .writeCommand("SET", 2)
                .writeAscii("COUNTER")
                .writeAscii("5")

                .writeCommand("INCRBY", 2)
                .writeAscii("COUNTER")
                .writeInt(10)

                .writeCommand("INCRBY", 2)
                .writeAscii("COUNTER")
                .writeInt(-10)

                .writeCommand("DEL", 1)
                .writeAscii("COUNTER")

                .flushAndRead();

        assertEquals("OK", redis.nextString()); // SET
        assertEquals(15, redis.nextInt()); // INCRBY
        assertEquals(5, redis.nextInt()); // INCRBY
        assertEquals(1, redis.nextInt()); // DEL
    }

    @Test
    void readArray() {
        redis
                .writeCommand("DEL", 1)
                .writeAscii("VALUES");

        val values = new HashSet<String>();

        for (int i = 0; i < 10; i++) {
            val value = String.valueOf(i);
            values.add(value);

            redis
                    .writeCommand("SADD", 2)
                    .writeAscii("VALUES")
                    .writeAscii(value);
        }

        redis
                .writeCommand("SMEMBERS", 1)
                .writeAscii("VALUES")
                .flushAndRead();

        redis.skip(11); // del + 10 sadd

        val result = new HashSet<String>();

        for (int i = 0, j = redis.nextArray(); i < j; i++) {
            result.add(redis.nextString());
        }

        assertEquals(10, result.size());
        assertEquals(result, values);
    }

    @Test
    void readNumber() {
        redis
                .writeCommand("DEL", 1)
                .writeAscii("VALUES")

                .writeCommand("SCARD", 1)
                .writeAscii("VALUES")

                .writeCommand("SADD", 2)
                .writeAscii("VALUES")
                .writeAscii("ELEMENT")

                .writeCommand("SCARD", 1)
                .writeAscii("VALUES")
                .flushAndRead();

        redis.skip(); // del
        assertEquals(0, redis.nextInt());
        redis.skip(); // sadd
        assertEquals(1, redis.nextInt());
    }

}
