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

import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;

/**
 * @author whilein
 */
public interface Redis extends AutoCloseable {

    boolean isAvailable();

    void setAsciiWriter(@NotNull AsciiWriter asciiWriter);

    void flush();

    @NotNull RedisResponse flushAndRead();

    @NotNull RedisResponse read();

    @NotNull Redis command(@NotNull String name, int arguments);

    @NotNull Redis argument(int number);

    @NotNull Redis argument(long number);

    @NotNull Redis argument(@NotNull String text, @NotNull Charset charset);

    @NotNull Redis argument(@NotNull String text);

    @NotNull Redis argument(byte @NotNull [] bytes);

    @NotNull Redis command(@NotNull String name);

}
