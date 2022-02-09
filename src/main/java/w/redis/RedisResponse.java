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

/**
 * @author whilein
 */
public interface RedisResponse {

    boolean hasNext();

    boolean isError();

    int nextArray();

    @NotNull String nextString();

    byte @NotNull [] nextBytes();

    void skip();

    void skip(int count);

    int nextBytes(byte @NotNull [] bytes);

    int nextBytes(byte @NotNull [] bytes, int off, int len);

    int nextInt();

    long nextLong();

}
