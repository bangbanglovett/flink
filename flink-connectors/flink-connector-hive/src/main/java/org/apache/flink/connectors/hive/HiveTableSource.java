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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.read.HiveContinuousMonitoringFunction;
import org.apache.flink.connectors.hive.read.HiveTableFileInputFormat;
import org.apache.flink.connectors.hive.read.HiveTableInputFormat;
import org.apache.flink.connectors.hive.read.TimestampedHiveInputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperatorFactory;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.catalog.hive.util.HivePartitionUtils.getAllPartitions;
import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.checkAcidTable;
import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toLocalDateTime;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_CONSUME_START_OFFSET;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_ORDER;

/**
 * A TableSource implementation to read data from Hive tables.
 */
public class HiveTableSource implements
		ScanTableSource,
		SupportsPartitionPushDown,
		SupportsProjectionPushDown,
		SupportsLimitPushDown {

	private static final Logger LOG = LoggerFactory.getLogger(HiveTableSource.class);

	protected final JobConf jobConf;
	protected final ReadableConfig flinkConf;
	protected final ObjectPath tablePath;
	protected final CatalogTable catalogTable;
	protected final String hiveVersion;
	protected final HiveShim hiveShim;

	// Remaining partition specs after partition pruning is performed. Null if pruning is not pushed down.
	@Nullable
	private List<Map<String, String>> remainingPartitions = null;
	protected int[] projectedFields;
	private long limit = -1L;

	public HiveTableSource(
			JobConf jobConf, ReadableConfig flinkConf, ObjectPath tablePath, CatalogTable catalogTable) {
		this.jobConf = Preconditions.checkNotNull(jobConf);
		this.flinkConf = Preconditions.checkNotNull(flinkConf);
		this.tablePath = Preconditions.checkNotNull(tablePath);
		this.catalogTable = Preconditions.checkNotNull(catalogTable);
		this.hiveVersion = Preconditions.checkNotNull(jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
				"Hive version is not defined");
		this.hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		return new DataStreamScanProvider() {
			@Override
			public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
				return getDataStream(execEnv);
			}

			@Override
			public boolean isBounded() {
				return !isStreamingSource();
			}
		};
	}

	@VisibleForTesting
	protected DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
		checkAcidTable(catalogTable, tablePath);
		List<HiveTablePartition> allHivePartitions = getAllPartitions(
				jobConf,
				hiveVersion,
				tablePath,
				catalogTable,
				hiveShim,
				remainingPartitions);

		@SuppressWarnings("unchecked")
		TypeInformation<RowData> typeInfo =
				(TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(getProducedDataType());

		HiveTableInputFormat inputFormat = getInputFormat(
				allHivePartitions,
				flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER));

		if (isStreamingSource()) {
			if (catalogTable.getPartitionKeys().isEmpty()) {
				return createStreamSourceForNonPartitionTable(execEnv, typeInfo, inputFormat, allHivePartitions.get(0));
			} else {
				return createStreamSourceForPartitionTable(execEnv, typeInfo, inputFormat);
			}
		} else {
			return createBatchSource(execEnv, typeInfo, inputFormat);
		}
	}

	protected boolean isStreamingSource() {
		return Boolean.parseBoolean(catalogTable.getOptions().getOrDefault(
				STREAMING_SOURCE_ENABLE.key(),
				STREAMING_SOURCE_ENABLE.defaultValue().toString()));
	}

	private DataStream<RowData> createBatchSource(StreamExecutionEnvironment execEnv,
			TypeInformation<RowData> typeInfo, HiveTableInputFormat inputFormat) {
		DataStreamSource<RowData> source = execEnv.createInput(inputFormat, typeInfo);

		int parallelism = new HiveParallelismInference(tablePath, flinkConf)
				.infer(inputFormat::getNumFiles, () -> inputFormat.createInputSplits(0).length)
				.limit(limit);

		source.setParallelism(parallelism);
		return source.name("HiveSource-" + tablePath.getFullName());
	}

	private DataStream<RowData> createStreamSourceForPartitionTable(
			StreamExecutionEnvironment execEnv,
			TypeInformation<RowData> typeInfo,
			HiveTableInputFormat inputFormat) {
		Configuration configuration = new Configuration();
		catalogTable.getOptions().forEach(configuration::setString);

		String consumeOrderStr = configuration.get(STREAMING_SOURCE_PARTITION_ORDER);
		ConsumeOrder consumeOrder = ConsumeOrder.getConsumeOrder(consumeOrderStr);
		String consumeOffset = configuration.get(STREAMING_SOURCE_CONSUME_START_OFFSET);
		String extractorKind = configuration.get(PARTITION_TIME_EXTRACTOR_KIND);
		String extractorClass = configuration.get(PARTITION_TIME_EXTRACTOR_CLASS);
		String extractorPattern = configuration.get(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);
		Duration monitorInterval = configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);

		HiveContinuousMonitoringFunction monitoringFunction = new HiveContinuousMonitoringFunction(
				hiveShim,
				jobConf,
				tablePath,
				catalogTable,
				execEnv.getParallelism(),
				consumeOrder,
				consumeOffset,
				extractorKind,
				extractorClass,
				extractorPattern,
				monitorInterval.toMillis());

		ContinuousFileReaderOperatorFactory<RowData, TimestampedHiveInputSplit> factory =
				new ContinuousFileReaderOperatorFactory<>(inputFormat);

		String sourceName = "HiveMonitoringFunction";
		SingleOutputStreamOperator<RowData> source = execEnv
				.addSource(monitoringFunction, sourceName)
				.transform("Split Reader: " + sourceName, typeInfo, factory);

		return new DataStreamSource<>(source);
	}

	private DataStream<RowData> createStreamSourceForNonPartitionTable(
			StreamExecutionEnvironment execEnv,
			TypeInformation<RowData> typeInfo,
			HiveTableInputFormat inputFormat,
			HiveTablePartition hiveTable) {
		HiveTableFileInputFormat fileInputFormat = new HiveTableFileInputFormat(inputFormat, hiveTable);

		Configuration configuration = new Configuration();
		catalogTable.getOptions().forEach(configuration::setString);
		String consumeOrderStr = configuration.get(STREAMING_SOURCE_PARTITION_ORDER);
		ConsumeOrder consumeOrder = ConsumeOrder.getConsumeOrder(consumeOrderStr);
		if (consumeOrder != ConsumeOrder.CREATE_TIME_ORDER) {
			throw new UnsupportedOperationException(
					"Only " + ConsumeOrder.CREATE_TIME_ORDER + " is supported for non partition table.");
		}

		String consumeOffset = configuration.get(STREAMING_SOURCE_CONSUME_START_OFFSET);
		// to Local zone mills instead of UTC mills
		long currentReadTime = TimestampData.fromLocalDateTime(toLocalDateTime(consumeOffset))
				.toTimestamp().getTime();

		Duration monitorInterval = configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);

		ContinuousFileMonitoringFunction<RowData> monitoringFunction =
				new ContinuousFileMonitoringFunction<>(
						fileInputFormat,
						FileProcessingMode.PROCESS_CONTINUOUSLY,
						execEnv.getParallelism(),
						monitorInterval.toMillis(),
						currentReadTime);

		ContinuousFileReaderOperatorFactory<RowData, TimestampedFileInputSplit> factory =
				new ContinuousFileReaderOperatorFactory<>(fileInputFormat);

		String sourceName = "HiveFileMonitoringFunction";
		SingleOutputStreamOperator<RowData> source = execEnv.addSource(monitoringFunction, sourceName)
				.transform("Split Reader: " + sourceName, typeInfo, factory);

		return new DataStreamSource<>(source);
	}

	@VisibleForTesting
	HiveTableInputFormat getInputFormat(List<HiveTablePartition> allHivePartitions, boolean useMapRedReader) {
		return new HiveTableInputFormat(
				jobConf,
				catalogTable,
				allHivePartitions,
				projectedFields,
				limit,
				hiveVersion,
				useMapRedReader);
	}

	protected TableSchema getTableSchema() {
		return catalogTable.getSchema();
	}

	private DataType getProducedDataType() {
		return getProducedTableSchema().toRowDataType().bridgedTo(RowData.class);
	}

	protected TableSchema getProducedTableSchema() {
		TableSchema fullSchema = getTableSchema();
		if (projectedFields == null) {
			return fullSchema;
		} else {
			String[] fullNames = fullSchema.getFieldNames();
			DataType[] fullTypes = fullSchema.getFieldDataTypes();
			return TableSchema.builder().fields(
					Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
					Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new)).build();
		}
	}

	@Override
	public void applyLimit(long limit) {
		this.limit = limit;
	}

	@Override
	public Optional<List<Map<String, String>>> listPartitions() {
		return Optional.empty();
	}

	@Override
	public void applyPartitions(List<Map<String, String>> remainingPartitions) {
		if (catalogTable.getPartitionKeys() != null && catalogTable.getPartitionKeys().size() != 0) {
			this.remainingPartitions = remainingPartitions;
		} else {
			throw new UnsupportedOperationException(
					"Should not apply partitions to a non-partitioned table.");
		}
	}

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.projectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
	}

	@Override
	public String asSummaryString() {
		return "HiveSource";
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public DynamicTableSource copy() {
		HiveTableSource source = new HiveTableSource(jobConf, flinkConf, tablePath, catalogTable);
		source.remainingPartitions = remainingPartitions;
		source.projectedFields = projectedFields;
		source.limit = limit;
		return source;
	}
}
