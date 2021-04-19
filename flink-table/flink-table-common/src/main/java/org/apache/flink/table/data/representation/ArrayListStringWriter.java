/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.representation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;

import java.lang.reflect.Array;
import java.time.ZoneId;
import java.util.List;

import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;

/** StringWriter for {@link ArrayType} of {@link List} external type. */
@Internal
public class ArrayListStringWriter<E> implements DataStructureStringWriter<List<E>> {

    private final DataType dataType;
    private final ZoneId sessionTimeZone;
    private final Object[] arrayKind;
    private final DataType elementType;
    private final Class<?> elementClazz;
    private final DataStructureStringWriter<Object> elementStringWriter;

    public ArrayListStringWriter(DataType dataType, ZoneId sessionTimeZone) {
        this.dataType = dataType;
        this.sessionTimeZone = sessionTimeZone;
        this.elementType = ((CollectionDataType) dataType).getElementDataType();
        this.elementClazz = elementType.getConversionClass();
        this.elementStringWriter =
                DataStructureStringWriters.getDataStructureStringWriter(
                        elementType, sessionTimeZone);
        this.arrayKind = createObjectArrayKind(elementClazz);
    }

    @Override
    public String representString(List<E> data) {
        if (data == null) {
            return "null";
        }
        Object[] wrapData = data.toArray(arrayKind);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        for (int i = 0; i < wrapData.length; i++) {
            if (i > 0) {
                stringBuilder.append(",");
            }

            stringBuilder.append(elementStringWriter.representString(wrapData[i]));
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    @SuppressWarnings({"unchecked"})
    public static DataStructureStringWriter create(DataType dataType, ZoneId sessionTimeZone) {
        return new ArrayListStringWriter(dataType, sessionTimeZone);
    }

    /** Creates the kind of array for {@link List#toArray(Object[])}. */
    private static Object[] createObjectArrayKind(Class<?> elementClazz) {
        // e.g. int[] is not a Object[]
        if (elementClazz.isPrimitive()) {
            return (Object[]) Array.newInstance(primitiveToWrapper(elementClazz), 0);
        }
        // e.g. int[][] and Integer[] are Object[]
        return (Object[]) Array.newInstance(elementClazz, 0);
    }
}
