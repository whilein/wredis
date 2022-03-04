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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.FieldDefaults;

import java.util.Arrays;

/**
 * @author whilein
 */
@Getter
@FieldDefaults(level = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractRedisBuffer implements RedisBuffer {

    byte[] array;

    @Setter
    int position;

    @Override
    public int getCapacity() {
        return array.length;
    }

    @Override
    public void resize() {
        resize(array.length * 2);
    }

    @Override
    public void resize(final int to) {
        array = Arrays.copyOf(array, to);
    }
}
