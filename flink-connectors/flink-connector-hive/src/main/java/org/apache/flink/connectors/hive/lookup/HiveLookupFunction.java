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

package org.apache.flink.connectors.hive.lookup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.ConsumeOrder;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.connectors.hive.read.PartitionFetcher;
import org.apache.flink.connectors.hive.read.PartitionReader;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionTimeExtractor;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toMills;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_ORDER;

/**
 * Lookup function for filesystem connector tables.
 *
 * <p>The hive connector and filesystem connector share read/write files code.
 * Currently, only this function only used in hive connector.
 */
public class HiveLookupFunction<P> extends TableFunction<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(HiveLookupFunction.class);

	// the max number of retries before throwing exception, in case of failure to load the table into cache
	private static final int MAX_RETRIES = 3;
	// interval between retries
	private static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

	private final PartitionFetcher<P> partitionFetcher;
	private final PartitionReader<P, RowData> partitionReader;
	private final int[] lookupCols;
	private final RowData.FieldGetter[] lookupFieldGetters;
	private final Duration cacheTTL;
	private final HiveTableConfig hiveTableConfig;
	private final TypeSerializer<RowData> serializer;

	// cache for lookup data
	private transient Map<RowData, List<RowData>> cache;
	// timestamp when cache expires
	private transient long nextLoadTime;
	private transient PartitionFetcher.Context partitionFetcherContext;
	private transient IMetaStoreClient metaStoreClient;

	public HiveLookupFunction(
			HiveTableConfig hiveTableConfig,
			PartitionFetcher<P> partitionFetcher,
			PartitionReader<P, RowData> partitionReader,
			String[] lookupKeys,
			Duration cacheTTL) {
		this.hiveTableConfig = hiveTableConfig;
		this.cacheTTL = cacheTTL;
		this.partitionFetcher = partitionFetcher;
		this.partitionReader = partitionReader;
		this.lookupCols = new int[lookupKeys.length];
		this.lookupFieldGetters = new RowData.FieldGetter[lookupKeys.length];
		Map<String, Integer> nameToIndex = IntStream.range(0, hiveTableConfig.getFieldNames().length).boxed().collect(
				Collectors.toMap(i -> hiveTableConfig.getFieldNames()[i], i -> i));
		for (int i = 0; i < lookupKeys.length; i++) {
			Integer index = nameToIndex.get(lookupKeys[i]);
			Preconditions.checkArgument(index != null, "Lookup keys %s not selected", Arrays.toString(lookupKeys));
			lookupFieldGetters[i] = RowData.createFieldGetter(hiveTableConfig.getFieldTypes()[index].getLogicalType(), index);
			lookupCols[i] = index;
		}
		this.serializer = getResultType().createSerializer(new ExecutionConfig());
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		cache = new HashMap<>();
		nextLoadTime = -1L;
		initFetcherContext();
	}

	private void initFetcherContext() throws Exception {
		this.metaStoreClient = hiveTableConfig.getHiveShim().getHiveMetastoreClient(new HiveConf(hiveTableConfig.getConf(), HiveConf.class));
		Table table = metaStoreClient.getTable(hiveTableConfig.getTablePath().getDatabaseName(), hiveTableConfig.getTablePath().getObjectName());
		StorageDescriptor tableSd = table.getSd();
		Properties tableProps = HiveReflectionUtils.getTableMetadata(hiveTableConfig.getHiveShim(), table);

		Configuration configuration = hiveTableConfig.getConfiguration();
		String consumeOrderStr = configuration.get(STREAMING_SOURCE_PARTITION_ORDER);
		ConsumeOrder consumeOrder = ConsumeOrder.getConsumeOrder(consumeOrderStr);
		String extractorKind = configuration.get(PARTITION_TIME_EXTRACTOR_KIND);
		String extractorClass = configuration.get(PARTITION_TIME_EXTRACTOR_CLASS);
		String extractorPattern = configuration.get(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);

		PartitionTimeExtractor extractor = PartitionTimeExtractor.create(
				Thread.currentThread().getContextClassLoader(),
				extractorKind,
				extractorClass,
				extractorPattern);

		Path location = new Path(table.getSd().getLocation());
		FileSystem fs = location.getFileSystem(hiveTableConfig.getConf());

		this.partitionFetcherContext = new PartitionFetcher.Context() {

			@Override
			public List<String> partitionKeys() {
				return hiveTableConfig.getPartitionKeys();
			}

			@Override
			public Optional<Partition> getPartition(List<String> partValues) throws TException {
				try {
					return Optional.of(metaStoreClient.getPartition(
							hiveTableConfig.getTablePath().getDatabaseName(),
							hiveTableConfig.getTablePath().getObjectName(),
							partValues));
				} catch (NoSuchObjectException e) {
					return Optional.empty();
				}
			}

			@Override
			public FileSystem fileSystem() {
				return fs;
			}

			@Override
			public Path tableLocation() {
				return new Path(table.getSd().getLocation());
			}

			@Override
			public StorageDescriptor tableSd() {
				return tableSd;
			}

			@Override
			public Properties tableProps() {
				return tableProps;
			}

			@Override
			public long extractTimestamp(
					List<String> partKeys,
					List<String> partValues,
					Supplier<Long> fileTime) {
				switch (consumeOrder) {
					case CREATE_TIME_ORDER:
						return fileTime.get();
					case PARTITION_TIME_ORDER:
						return toMills(extractor.extract(partKeys, partValues));
					default:
						throw new UnsupportedOperationException(
								"Unsupported consumer order: " + consumeOrder);
				}
			}
		};
	}

	@Override
	public TypeInformation<RowData> getResultType() {
		return InternalTypeInfo.ofFields(
				Arrays.stream(hiveTableConfig.getFieldTypes()).map(DataType::getLogicalType).toArray(LogicalType[]::new),
				hiveTableConfig.getFieldNames());
	}

	public void eval(Object... values) {
		checkCacheReload();
		RowData lookupKey = GenericRowData.of(values);
		List<RowData> matchedRows = cache.get(lookupKey);
		if (matchedRows != null) {
			for (RowData matchedRow : matchedRows) {
				collect(matchedRow);
			}
		}
	}

	private void checkCacheReload() {
		if (nextLoadTime > System.currentTimeMillis()) {
			return;
		}
		if (nextLoadTime > 0) {
			LOG.info("Lookup join cache has expired after {} minute(s), reloading", cacheTTL.toMinutes());
		} else {
			LOG.info("Populating lookup join cache");
		}
		int numRetry = 0;
		while (true) {
			cache.clear();
			try {
				long count = 0;
				GenericRowData reuse = new GenericRowData(hiveTableConfig.getFieldNames().length);
				partitionReader.open(partitionFetcher.fetch(partitionFetcherContext));
				while (partitionReader.hasNext()) {
					RowData row = partitionReader.nextRecord(reuse);
					count++;
					RowData key = extractLookupKey(row);
					List<RowData> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
					rows.add(serializer.copy(row));
				}
				partitionReader.close();
				nextLoadTime = System.currentTimeMillis() + cacheTTL.toMillis();
				LOG.info("Loaded {} row(s) into lookup join cache", count);
				return;
			} catch (Exception e) {
				if (numRetry >= MAX_RETRIES) {
					throw new FlinkRuntimeException(
							String.format("Failed to load table into cache after %d retries", numRetry), e);
				}
				numRetry++;
				long toSleep = numRetry * RETRY_INTERVAL.toMillis();
				LOG.warn(String.format("Failed to load table into cache, will retry in %d seconds", toSleep / 1000), e);
				try {
					Thread.sleep(toSleep);
				} catch (InterruptedException ex) {
					LOG.warn("Interrupted while waiting to retry failed cache load, aborting");
					throw new FlinkRuntimeException(ex);
				}
			}
		}
	}

	private RowData extractLookupKey(RowData row) {
		GenericRowData key = new GenericRowData(lookupCols.length);
		for (int i = 0; i < lookupCols.length; i++) {
			key.setField(i, lookupFieldGetters[i].getFieldOrNull(row));
		}
		return key;
	}

	@Override
	public void close() throws Exception {
		if (this.metaStoreClient != null) {
			this.metaStoreClient.close();
		}
	}

	@VisibleForTesting
	public Duration getCacheTTL() {
		return cacheTTL;
	}

	@VisibleForTesting
	public PartitionFetcher<P> getPartitionFetcher() {
		return partitionFetcher;
	}

	@VisibleForTesting
	public PartitionReader<P, RowData> getPartitionReader() {
		return partitionReader;
	}

	@VisibleForTesting
	public PartitionFetcher.Context getPartitionFetcherContext() {
		return partitionFetcherContext;
	}


	/**
	 * Class that Stores general configurations which used to create a hive table.
	 */
	public static class HiveTableConfig implements Serializable {

		private static final long serialVersionUID = 1L;
		private final ObjectPath tablePath;
		private final HiveShim hiveShim;
		private final JobConfWrapper confWrapper;
		private final List<String> partitionKeys;
		private final DataType[] fieldTypes;
		private final String[] fieldNames;
		private final Configuration configuration;

		public HiveTableConfig(
				ObjectPath tablePath,
				HiveShim hiveShim,
				JobConfWrapper confWrapper,
				List<String> partitionKeys,
				DataType[] fieldTypes,
				String[] fieldNames,
				Configuration configuration) {
			this.tablePath = tablePath;
			this.hiveShim = hiveShim;
			this.confWrapper = confWrapper;
			this.partitionKeys = partitionKeys;
			this.fieldTypes = fieldTypes;
			this.fieldNames = fieldNames;
			this.configuration = configuration;
		}

		public ObjectPath getTablePath() {
			return tablePath;
		}

		public HiveShim getHiveShim() {
			return hiveShim;
		}

		public JobConf getConf() {
			return confWrapper.conf();
		}

		public List<String> getPartitionKeys() {
			return partitionKeys;
		}

		public DataType[] getFieldTypes() {
			return fieldTypes;
		}

		public String[] getFieldNames() {
			return fieldNames;
		}

		public Configuration getConfiguration() {
			return configuration;
		}
	}

}


