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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import w.redis.buffer.WriteRedisBuffer;

import java.nio.ByteBuffer;

/**
 * Запись {@code Ascii} строки в {@link ByteBuffer}.
 * <p>
 * Если у вас есть возможность использовать {@code Unsafe}, вы можете вытащить из
 * строки {@link String}{@code .value} и записать его полностью в {@link ByteBuffer} без итерации.
 *
 * @author whilein
 */
public interface AsciiWriter {

    static AsciiWriter defaultAsciiWriter() {
        return Default.INSTANCE;
    }

    void write(String text, WriteRedisBuffer buffer);

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    final class Default implements AsciiWriter {

        private static final AsciiWriter INSTANCE = new Default();

        @Override
        public void write(final String text, final WriteRedisBuffer buffer) {
            for (int i = 0, j = text.length(); i < j; i++) {
                buffer.writeRaw((byte) text.charAt(i));
            }
        }

    }

}
