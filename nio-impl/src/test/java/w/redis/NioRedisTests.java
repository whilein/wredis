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
import w.redis.nio.NioRedis;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * @author whilein
 */
final class NioRedisTests {

    static Redis redis;

    @BeforeAll
    static void setup() {
        boolean connected;

        try {
            redis = NioRedis.builder(new InetSocketAddress("localhost", 6379))
                    .connectTimeout(1, TimeUnit.SECONDS)
                    .connect();

            connected = true;
        } catch (final Exception e) {
            connected = false;
        }

        assumeTrue(connected);
    }

    @Test
    void readString() {
        val response = redis.writeCommand("PING").flushAndRead();
        assertEquals("PONG", response.nextString());
    }

    @Test
    void longArgument() {
        val response = redis
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

        assertEquals("OK", response.nextString()); // SET
        assertEquals(5, response.nextLong()); // DECRBY
        assertEquals(15, response.nextLong()); // DECRBY
        assertEquals(1, response.nextInt()); // DEL
    }

    @Test
    void intArgument() {
        val response = redis
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

        assertEquals("OK", response.nextString()); // SET
        assertEquals(15, response.nextInt()); // INCRBY
        assertEquals(5, response.nextInt()); // INCRBY
        assertEquals(1, response.nextInt()); // DEL
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
                .writeAscii("VALUES");

        val response = redis.flushAndRead();
        response.skip(11); // del + 10 sadd

        val result = new HashSet<String>();

        for (int i = 0, j = response.nextArray(); i < j; i++) {
            result.add(response.nextString());
        }

        assertEquals(10, result.size());
        assertEquals(result, values);
    }

    @Test
    void readNumber() {
        val response = redis
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

        response.skip(); // del
        assertEquals(0, response.nextInt());
        response.skip(); // sadd
        assertEquals(1, response.nextInt());
    }

}
