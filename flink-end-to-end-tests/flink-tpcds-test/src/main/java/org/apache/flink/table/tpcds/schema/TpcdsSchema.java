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

package org.apache.flink.table.tpcds.schema;

import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.stream.Collectors;

/** Class to define table schema of TPS-DS table. */
public class TpcdsSchema implements Schema {
	List<Column> columns;

	public TpcdsSchema(List<Column> columns) {
		this.columns = columns;
	}

	@Override
	public List<String> getFieldNames() {
		return columns.stream().map(column -> column.getName()).collect(Collectors.toList());
	}

	@Override
	public List<DataType> getFieldTypes() {
		return columns.stream().map(column -> column.getDataType()).collect(Collectors.toList());
	}
}
