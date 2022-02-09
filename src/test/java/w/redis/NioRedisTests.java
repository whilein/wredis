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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * @author whilein
 */
final class NioRedisTests {

    static Redis redis;

    @BeforeAll
    static void setup() {
        redis = NioRedis.create(new InetSocketAddress("127.0.0.1", 6379));

        assumeTrue(redis.isAvailable());
    }

    @Test
    void readString() {
        redis.command("PING");
        redis.flush();
    }

    @Test
    void readArray() {
        redis.command("DEL", 1).argument("VALUES");

        val values = new HashSet<String>();

        for (int i = 0; i < 10; i++) {
            val value = String.valueOf(i);
            values.add(value);

            redis.command("SADD", 2).argument("VALUES").argument(value);
        }

        redis.command("SMEMBERS", 1).argument("VALUES");
        redis.flush();

        val response = redis.read();
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
        redis.command("DEL", 1).argument("VALUES");
        redis.command("SCARD", 1).argument("VALUES");
        redis.command("SADD", 2).argument("VALUES").argument("ELEMENT");
        redis.command("SCARD", 1).argument("VALUES");

        redis.flush();

        val response = redis.read();
        response.skip(); // del
        assertEquals(0, response.nextInt());
        response.skip(); // sadd
        assertEquals(1, response.nextInt());
    }

}
