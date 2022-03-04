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

import org.jetbrains.annotations.NotNull;

/**
 * @author whilein
 */
public interface WriteRedisBuffer extends RedisBuffer {

    void ensure(int length);

    void writeCrlf();

    void writeCommand(@NotNull String command, int arguments);

    void writeAscii(@NotNull String text);

    void writeUTF(@NotNull String text);

    void writeRaw(byte value);

    void writeRaw(byte @NotNull [] value);

    void writeEmptyString();

    void writeBytes(byte @NotNull [] bytes);

    void writeInt(int value);

    void writeLong(long value);

}
