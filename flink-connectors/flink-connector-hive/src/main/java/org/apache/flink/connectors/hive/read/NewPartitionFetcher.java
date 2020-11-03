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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.hive.util.HivePartitionUtils;
import org.apache.flink.table.data.TimestampData;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionValues;

/**
 * Partition fetcher that fetches new partitions by compared to previous timestamp.
 **/
public class NewPartitionFetcher implements PartitionFetcher<Tuple2<Partition, Long>> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<Tuple2<Partition, Long>> fetch(Context context) throws Exception {
		FileStatus[] statuses = HivePartitionUtils.getFileStatusRecurse(
				context.tableLocation(), context.partitionKeys().size(), context.fileSystem());
		List<Tuple2<List<String>, Long>> partValueList = suitablePartitions(context, context.previousTimestamp(), statuses);

		List<Tuple2<Partition, Long>> partitions = new ArrayList<>();
		for (Tuple2<List<String>, Long> tuple2 : partValueList) {
			context.getPartition(tuple2.f0).ifPresent(
					partition -> partitions.add(new Tuple2<>(partition, tuple2.f1)));
		}
		return partitions;
	}

	/**
	 * Find suitable partitions, extract timestamp and compare it with previousTimestamp.
	 */
	@VisibleForTesting
	static List<Tuple2<List<String>, Long>> suitablePartitions(
			Context context,
			long previousTimestamp,
			FileStatus[] statuses) {
		List<Tuple2<List<String>, Long>> partValueList = new ArrayList<>();
		for (FileStatus status : statuses) {
			List<String> partValues = extractPartitionValues(
					new org.apache.flink.core.fs.Path(status.getPath().toString()));
			long timestamp = context.extractTimestamp(
					context.partitionKeys(),
					partValues,
					// to UTC millisecond.
					() -> TimestampData.fromTimestamp(
							new Timestamp(status.getModificationTime())).getMillisecond());
			if (timestamp >= previousTimestamp) {
				partValueList.add(new Tuple2<>(partValues, timestamp));
			}
		}
		return partValueList;
	}
}
