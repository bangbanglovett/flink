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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.lookup.HiveLookupFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.ExceptionUtils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;

import static org.apache.flink.table.filesystem.FileSystemOptions.LOOKUP_JOIN_CACHE_TTL;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_CONSUME_START_OFFSET;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_INCLUDE;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_ORDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link HiveDynamicTableFactory}.
 */
public class HiveDynamicTableFactoryTest {

	private static TableEnvironment tableEnv;
	private static HiveCatalog hiveCatalog;

	@BeforeClass
	public static void setup() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		tableEnv = TableEnvironment.create(settings);
		hiveCatalog = HiveTestUtils.createHiveCatalog();
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
	}

	@Test
	public void testHiveStreamingSourceOptions() throws Exception {
		// test default hive streaming-source is not a lookup source
		tableEnv.executeSql(String.format(
				"create table table1 (x int, y string, z int) partitioned by (" +
						" pt_year int, pt_mon string, pt_day string)" +
						" tblproperties ('%s' = 'true')",
				STREAMING_SOURCE_ENABLE.key()
		));
		DynamicTableSource tableSource1 = getTableSource("table1");
		assertFalse(tableSource1 instanceof HiveLookupTableSource);

		// test deprecated option key 'streaming-source.consume-order' and new key 'streaming-source.partition-order'
		tableEnv.executeSql(String.format(
				"create table table2 (x int, y string, z int) partitioned by (" +
						" pt_year int, pt_mon string, pt_day string)" +
						" tblproperties ('%s' = 'true', '%s' = 'partition-time')",
				STREAMING_SOURCE_ENABLE.key(),
				"streaming-source.consume-order"
		));
		DynamicTableSource tableSource2 = getTableSource("table2");
		assertTrue(tableSource2 instanceof HiveTableSource);
		HiveTableSource hiveTableSource = (HiveTableSource) tableSource2;

		Configuration configuration = new Configuration();
		hiveTableSource.catalogTable.getOptions().forEach(configuration::setString);
		String partitionOrder = configuration.get(STREAMING_SOURCE_PARTITION_ORDER);
		assertEquals("partition-time", partitionOrder);
	}

	@Test
	public void testHiveLookupSourceOptions() throws Exception {
		// test hive bounded source is a lookup source
		tableEnv.executeSql(String.format(
				"create table table3 (x int, y string, z int) tblproperties ('%s'='5min')",
				LOOKUP_JOIN_CACHE_TTL.key()));
		DynamicTableSource tableSource1 = getTableSource("table3");
		assertTrue(tableSource1 instanceof HiveLookupTableSource);

		// test hive streaming source is a lookup source when 'streaming-source.partition.include' = 'latest'
		tableEnv.executeSql(String.format(
				"create table table4 (x int, y string, z int)" +
						" tblproperties ('%s' = 'true', '%s' = 'latest')",
				STREAMING_SOURCE_ENABLE.key(),
				STREAMING_SOURCE_PARTITION_INCLUDE.key()));
		DynamicTableSource tableSource2 = getTableSource("table4");
		assertTrue(tableSource2 instanceof HiveLookupTableSource);
		HiveLookupFunction lookupFunction = (HiveLookupFunction) ((HiveLookupTableSource) tableSource2)
				.getLookupFunction(new int[][]{{0}});
		// test default lookup cache ttl for streaming-source is 1 hour
		assertEquals(Duration.ofHours(1), lookupFunction.getCacheTTL());

		// test lookup with partition-time extractor options
		tableEnv.executeSql(String.format(
				"create table table5 (x int, y string, z int)" +
						" tblproperties (" +
						"'%s' = 'true'," +
						" '%s' = 'latest'," +
						" '%s' = '120min'," +
						" '%s' = 'partition-time', " +
						" '%s' = 'custom'," +
						" '%s' = 'path.to..TimeExtractor')",
				STREAMING_SOURCE_ENABLE.key(),
				STREAMING_SOURCE_PARTITION_INCLUDE.key(),
				STREAMING_SOURCE_MONITOR_INTERVAL.key(),
				STREAMING_SOURCE_PARTITION_ORDER.key(),
				PARTITION_TIME_EXTRACTOR_KIND.key(),
				PARTITION_TIME_EXTRACTOR_CLASS.key()
		));

		DynamicTableSource tableSource3 = getTableSource("table5");
		assertTrue(tableSource3 instanceof HiveLookupTableSource);
		HiveLookupTableSource tableSource = (HiveLookupTableSource) tableSource3;
		Configuration configuration = new Configuration();
		tableSource.catalogTable.getOptions().forEach(configuration::setString);

		assertEquals(configuration.get(STREAMING_SOURCE_PARTITION_ORDER), "partition-time");
		assertEquals(configuration.get(PARTITION_TIME_EXTRACTOR_KIND), "custom");
		assertEquals(configuration.get(PARTITION_TIME_EXTRACTOR_CLASS), "path.to..TimeExtractor");
	}

	@Test
	public void testInvalidOptions() {
		tableEnv.executeSql(String.format(
				"create table table6 (x int, y string, z int)" +
						" tblproperties ('%s' = 'true', '%s' = 'latest', '%s' = '5min')",
				STREAMING_SOURCE_ENABLE.key(),
				STREAMING_SOURCE_PARTITION_INCLUDE.key(),
				STREAMING_SOURCE_MONITOR_INTERVAL.key()));
		try {
			getTableSource("table6");
		} catch (Throwable t) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(t,
					"Currently the value of 'streaming-source.monitor-interval' is required bigger or " +
							"equal to default value '3600000' when set 'streaming-source.partition.include' to 'latest'," +
							" but actual is '300000'")
					.isPresent());
		}

		tableEnv.executeSql(String.format(
				"create table table7 (x int, y string, z int)" +
						" tblproperties ('%s' = 'true', '%s' = 'latest', '%s' = '120min', '%s' = '1970-00-01')",
				STREAMING_SOURCE_ENABLE.key(),
				STREAMING_SOURCE_PARTITION_INCLUDE.key(),
				STREAMING_SOURCE_MONITOR_INTERVAL.key(),
				STREAMING_SOURCE_CONSUME_START_OFFSET.key()));

		try {
			getTableSource("table7");
		} catch (Throwable t) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(t,
					"The 'streaming-source.consume-start-offset' is not supported when " +
							"set 'streaming-source.partition.include' to 'latest'")
					.isPresent());
		}
	}

	private DynamicTableSource getTableSource(String tableName) throws Exception {
		ObjectIdentifier tableIdentifier = ObjectIdentifier.of(hiveCatalog.getName(), "default", tableName);
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(tableIdentifier.toObjectPath());
		return FactoryUtil.createTableSource(
				hiveCatalog,
				tableIdentifier,
				catalogTable,
				tableEnv.getConfig().getConfiguration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}
}
