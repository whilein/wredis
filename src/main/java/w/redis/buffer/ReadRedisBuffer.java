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

import lombok.Getter;
import lombok.Setter;

/**
 * @author whilein
 */
public final class ReadRedisBuffer extends RedisBuffer {

    public ReadRedisBuffer(final byte[] array, final int position) {
        super(array, position);
    }

    @Getter
    @Setter
    int length;

    public byte getNext() {
        return array[position++];
    }

    public int remaining() {
        return length - position;
    }

    public boolean hasRemaining() {
        return position != length;
    }

}