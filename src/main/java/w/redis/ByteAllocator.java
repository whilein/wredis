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
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * @author whilein
 */
public interface ByteAllocator {

    static @NotNull ByteAllocator heapBufferAllocator() {
        return Heap.INSTANCE;
    }

    static @NotNull ByteAllocator directBufferAllocator() {
        return Direct.INSTANCE;
    }

    @NotNull ByteBuffer allocate(int capacity);

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    final class Heap implements ByteAllocator {

        private static final ByteAllocator INSTANCE = new Heap();

        @Override
        public @NotNull ByteBuffer allocate(final int capacity) {
            return ByteBuffer.allocate(capacity);
        }

    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    final class Direct implements ByteAllocator {

        private static final ByteAllocator INSTANCE = new Direct();

        @Override
        public @NotNull ByteBuffer allocate(final int capacity) {
            return ByteBuffer.allocateDirect(capacity);
        }

    }

}
