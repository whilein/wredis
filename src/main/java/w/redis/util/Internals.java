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

package w.redis.util;

import lombok.experimental.UtilityClass;
import lombok.val;
import sun.misc.Unsafe;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * @author whilein
 */
@UtilityClass
public class Internals {

    private static final VarHandle VH_VALUE;

    static {
        final MethodHandles.Lookup implLookup;

        // region IMPL_LOOKUP
        try {
            val theUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);

            val theUnsafe = (Unsafe) theUnsafeField.get(null);

            val implLookupField = MethodHandles.Lookup.class.getDeclaredField("IMPL_LOOKUP");

            implLookup = (MethodHandles.Lookup) theUnsafe.getObject(
                    theUnsafe.staticFieldBase(implLookupField),
                    theUnsafe.staticFieldOffset(implLookupField)
            );
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        try {
            VH_VALUE = implLookup.findVarHandle(String.class, "value", byte[].class);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getBytes(final String text) {
        return (byte[]) VH_VALUE.get(text);
    }

}
