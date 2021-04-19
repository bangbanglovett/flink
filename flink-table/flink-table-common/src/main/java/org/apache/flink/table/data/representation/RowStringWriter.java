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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

/** StringWriter for {@link RowType} of {@link Row} external type. */
@Internal
public class RowStringWriter implements DataStructureStringWriter<Row> {

    @Override
    public String representString(Row data) {
//		if (fieldType instanceof RowType && field instanceof Row) {
//			Row row = (Row) field;
//			Row normalizedRow = new Row(row.getKind(), row.getArity());
//			for (int i = 0; i < ((RowType) fieldType).getFields().size(); i++) {
//				LogicalType type = ((RowType) fieldType).getFields().get(i).getType();
//				normalizedRow.setField(
//						i, normalizeTimestamp(row.getField(i), type, sessionTimeZone));
//			}
//			return normalizedRow;
//
//		} else if (fieldType instanceof RowType && field instanceof RowData) {
//			RowData rowData = (RowData) field;
//			GenericRowData normalizedRowData =
//					new GenericRowData(rowData.getRowKind(), rowData.getArity());
//			for (int i = 0; i < ((RowType) fieldType).getFields().size(); i++) {
//				LogicalType type = ((RowType) fieldType).getFields().get(i).getType();
//				RowData.FieldGetter fieldGetter = RowData.createFieldGetter(type, i);
//				normalizedRowData.setField(
//						i,
//						normalizeTimestamp(
//								fieldGetter.getFieldOrNull(rowData),
//								type,
//								sessionTimeZone));
//			}
//			return normalizedRowData;
//
//        return null;
    }

    public static DataStructureStringWriter


}
