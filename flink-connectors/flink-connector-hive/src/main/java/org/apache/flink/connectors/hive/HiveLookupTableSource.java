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

package org.apache.flink.connectors.hive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.lookup.HiveLookupFunction;
import org.apache.flink.connectors.hive.read.HiveAllPartitionFetcher;
import org.apache.flink.connectors.hive.read.HiveInputFormatPartitionReader;
import org.apache.flink.connectors.hive.read.HiveLatestPartitionFetcher;
import org.apache.flink.connectors.hive.read.PartitionFetcher;
import org.apache.flink.connectors.hive.read.PartitionReader;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.filesystem.FileSystemOptions.LOOKUP_JOIN_CACHE_TTL;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_CONSUME_START_OFFSET;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_INCLUDE;

/**
 * Hive Table Source that has lookup ability.
 *
 * <p>Hive Table source has both lookup and continuous read ability, when it acts as continuous read source
 * it does not have the lookup ability but can be a temporal table just like other stream sources.
 * When it acts as bounded table, it has the lookup ability.
 *
 * <p>A common user case is use hive dimension table and always lookup the latest partition data, in this case
 * hive table source is a continuous read source but currently we implements it by LookupFunction. Because currently
 * TableSource can not tell the downstream when the latest partition has been read finished. This is a temporarily
 * workaround and will re-implement in the future.
 */
public class HiveLookupTableSource extends HiveTableSource implements LookupTableSource {

	private static final Duration DEFAULT_LOOKUP_MONITOR_INTERVAL = Duration.ofHours(1L);
	private final Configuration configuration;
	private Duration hiveTableCacheTTL;

	public HiveLookupTableSource(
			JobConf jobConf,
			ReadableConfig flinkConf,
			ObjectPath tablePath,
			CatalogTable catalogTable) {
		super(jobConf, flinkConf, tablePath, catalogTable);
		this.configuration = new Configuration();
		catalogTable.getOptions().forEach(configuration::setString);
		validateLookupConfigurations();
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		return TableFunctionProvider.of(getLookupFunction(context.getKeys()));
	}

	@VisibleForTesting
	TableFunction<RowData> getLookupFunction(int[][] keys) {
		List<String> keyNames = new ArrayList<>();
		TableSchema schema = getTableSchema();
		for (int[] key : keys) {
			if (key.length > 1) {
				throw new UnsupportedOperationException("Hive lookup can not support nested key now.");
			}
			keyNames.add(schema.getFieldName(key[0]).get());
		}
		return getLookupFunction(keyNames.toArray(new String[0]));
	}

	private void validateLookupConfigurations() {
		String partitionInclude = configuration.get(STREAMING_SOURCE_PARTITION_INCLUDE);
		if (isStreamingSource()) {
			Preconditions.checkArgument(
					!configuration.contains(STREAMING_SOURCE_CONSUME_START_OFFSET),
					String.format(
							"The '%s' is not supported when set '%s' to 'latest'",
							STREAMING_SOURCE_CONSUME_START_OFFSET.key(),
							STREAMING_SOURCE_PARTITION_INCLUDE.key()));

			Duration monitorInterval = configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);
			if (monitorInterval.equals(STREAMING_SOURCE_MONITOR_INTERVAL.defaultValue())) {
				monitorInterval = DEFAULT_LOOKUP_MONITOR_INTERVAL;
			}
			Preconditions.checkArgument(
					monitorInterval.toMillis() >= DEFAULT_LOOKUP_MONITOR_INTERVAL.toMillis(),
					String.format(
							"Currently the value of '%s' is required bigger or equal to default value '%s' " +
									"when set '%s' to 'latest', but actual is '%s'",
							STREAMING_SOURCE_MONITOR_INTERVAL.key(),
							DEFAULT_LOOKUP_MONITOR_INTERVAL.toMillis(),
							STREAMING_SOURCE_PARTITION_INCLUDE.key(),
							monitorInterval.toMillis())
			);

			hiveTableCacheTTL = monitorInterval;
		} else {
			Preconditions.checkArgument(
					"all".equals(partitionInclude),
					String.format("The only supported %s for lookup is '%s' in batch source," +
							" but actual is '%s'", STREAMING_SOURCE_PARTITION_INCLUDE.key(), "all", partitionInclude));

			hiveTableCacheTTL = configuration.get(LOOKUP_JOIN_CACHE_TTL);
		}
	}

	private TableFunction<RowData> getLookupFunction(String[] keys) {
		final HiveLookupFunction.HiveTableConfig hiveTableConfig = new HiveLookupFunction.HiveTableConfig(
				tablePath,
				hiveShim,
				new JobConfWrapper(jobConf),
				catalogTable.getPartitionKeys(),
				getProducedTableSchema().getFieldDataTypes(),
				getProducedTableSchema().getFieldNames(),
				configuration);

		final String defaultPartitionName = jobConf.get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
				HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);

		PartitionFetcher<HiveTablePartition> partitionFetcher;
		if (isStreamingSource() && !catalogTable.getPartitionKeys().isEmpty()) {
			partitionFetcher = new HiveLatestPartitionFetcher(
					getProducedTableSchema().getFieldDataTypes(),
					getProducedTableSchema().getFieldNames(),
					hiveShim,
					defaultPartitionName);

		} else {
			partitionFetcher = new HiveAllPartitionFetcher(
					getProducedTableSchema().getFieldDataTypes(),
					getProducedTableSchema().getFieldNames(),
					hiveShim,
					defaultPartitionName);
		}

		PartitionReader<HiveTablePartition, RowData> partitionReader = new HiveInputFormatPartitionReader(
				jobConf,
				hiveVersion,
				tablePath,
				getProducedTableSchema().getFieldDataTypes(),
				getProducedTableSchema().getFieldNames(),
				catalogTable.getPartitionKeys(),
				projectedFields,
				flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER));

		return new HiveLookupFunction<>(
				hiveTableConfig,
				partitionFetcher,
				partitionReader,
				keys,
				hiveTableCacheTTL);
	}
}
