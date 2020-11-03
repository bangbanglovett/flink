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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.catalog.hive.util.HivePartitionUtils.getAllPartValueToTimeList;
import static org.apache.flink.table.catalog.hive.util.HivePartitionUtils.toHiveTablePartition;

/**
 * Partition fetcher that fetches all partitions of the given hive table.
 */
public class HiveAllPartitionFetcher implements PartitionFetcher<HiveTablePartition> {

	private static final long serialVersionUID = 1L;
	private final DataType[] fieldTypes;
	private final String[] fieldNames;
	private final HiveShim shim;
	private final String defaultPartitionName;

	public HiveAllPartitionFetcher(
			DataType[] fieldTypes,
			String[] fieldNames,
			HiveShim shim,
			String defaultPartitionName) {
		this.fieldTypes = fieldTypes;
		this.fieldNames = fieldNames;
		this.shim = shim;
		this.defaultPartitionName = defaultPartitionName;
	}

	@Override
	public List<HiveTablePartition> fetch(Context context) throws Exception {
		List<HiveTablePartition> partValueList = new ArrayList<>();

		// non-partitioned table
		if (context.partitionKeys() != null && context.partitionKeys().isEmpty()) {
			partValueList.add(new HiveTablePartition(context.tableSd(), context.tableProps()));
		} else {
			List<Tuple2<List<String>, Long>> allPartValueToTime = getAllPartValueToTimeList(context);
			for (Tuple2<List<String>, Long> partValueToTime : allPartValueToTime) {
				context.getPartition(partValueToTime.f0)
						.ifPresent(
								partition -> partValueList.add(
										toHiveTablePartition(
												context.partitionKeys(),
												fieldNames,
												fieldTypes,
												shim,
												context.tableProps(),
												defaultPartitionName,
												partition))
						);
			}
		}
		return partValueList;
	}
}
