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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.connectors.jdbc.JdbcTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * ITCase for {@link JDBCTableSource}.
 */
public class JDBCTableSourceITCase extends AbstractTestBase {

	public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String DB_URL = "jdbc:derby:memory:test";
	public static final String INPUT_TABLE = "jdbcSource";

	@Before
	public void before() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", JdbcTestBase.class.getCanonicalName() + ".DEV_NULL");
		Class.forName(DRIVER_CLASS);

		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement statement = conn.createStatement()) {
			statement.executeUpdate("CREATE TABLE " + INPUT_TABLE + " (" +
					"id BIGINT NOT NULL," +
					"timestamp6_col TIMESTAMP, " +
					"timestamp9_col TIMESTAMP, " +
					"time_col TIME, " +
					"real_col FLOAT(23), " +    // A precision of 23 or less makes FLOAT equivalent to REAL.
					"double_col FLOAT(24)," +   // A precision of 24 or greater makes FLOAT equivalent to DOUBLE PRECISION.
					"decimal_col DECIMAL(10, 4))");
			statement.executeUpdate("INSERT INTO " + INPUT_TABLE + " VALUES (" +
					"1, TIMESTAMP('2020-01-01 15:35:00.123456'), TIMESTAMP('2020-01-01 15:35:00.123456789'), " +
					"TIME('15:35:00'), 1.175E-37, 1.79769E+308, 100.1234)");
			statement.executeUpdate("INSERT INTO " + INPUT_TABLE + " VALUES (" +
					"2, TIMESTAMP('2020-01-01 15:36:01.123456'), TIMESTAMP('2020-01-01 15:36:01.123456789'), " +
					"TIME('15:36:01'), -1.175E-37, -1.79769E+308, 101.1234)");
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL);
			Statement stat = conn.createStatement()) {
			stat.executeUpdate("DROP TABLE " + INPUT_TABLE);
		}
	}

	@Test
	public void testJDBCSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);
		JDBCTableSource tableSource = JDBCTableSource.builder()
			.setSchema(TableSchema.builder()
				.field("id", DataTypes.BIGINT())
				.field("timestamp6_col", DataTypes.TIMESTAMP(6))
				.field("timestamp9_col", DataTypes.TIMESTAMP(9))
				.field("time_col", DataTypes.TIME())
				.field("real_col", DataTypes.FLOAT())
				.field("double_col", DataTypes.DOUBLE())
				.field("decimal_col", DataTypes.DECIMAL(10, 4))
				.build())
			.setOptions(JDBCOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(INPUT_TABLE)
				.build())
			.build();

		JDBCTableSource projected = (JDBCTableSource) tableSource.projectFields(new int[] {1, 6});
		DataType expectedRowType = DataTypes.ROW(
			DataTypes.FIELD("timestamp6_col", DataTypes.TIMESTAMP(6)),
			DataTypes.FIELD("decimal_col", DataTypes.DECIMAL(10, 4)));
		Assert.assertEquals(expectedRowType.toString(), projected.getProducedDataType().toString());

		tEnv.registerTableSource(INPUT_TABLE, tableSource);

		StreamITCase.clear();
		tEnv.toAppendStream(tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE), Row.class)
			.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected =
			Arrays.asList(
				"1,2020-01-01T15:35:00.123456,2020-01-01T15:35:00.123456789,15:35,1.175E-37,1.79769E308,100.1234",
				"2,2020-01-01T15:36:01.123456,2020-01-01T15:36:01.123456789,15:36:01,-1.175E-37,-1.79769E308,101.1234");
		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testProjectableJDBCSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);
		JDBCTableSource tableSource = (JDBCTableSource) JDBCTableSource.builder()
			.setSchema(TableSchema.builder()
				.field("id", DataTypes.BIGINT())
				.field("timestamp6_col", DataTypes.TIMESTAMP(6))
				.field("timestamp9_col", DataTypes.TIMESTAMP(9))
				.field("time_col", DataTypes.TIME())
				.field("real_col", DataTypes.FLOAT())
				.field("double_col", DataTypes.DOUBLE())
				.field("decimal_col", DataTypes.DECIMAL(10, 4))
				.build())
			.setOptions(JDBCOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(INPUT_TABLE)
				.build())
			.build();

		tEnv.registerTableSource(INPUT_TABLE, tableSource);
		StreamITCase.clear();
		tEnv.toAppendStream(tEnv.sqlQuery("SELECT timestamp6_col, decimal_col FROM " + INPUT_TABLE), Row.class)
				.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected =
			Arrays.asList(
				"2020-01-01T15:35:00.123456,100.1234",
				"2020-01-01T15:36:01.123456,101.1234");
		StreamITCase.compareWithList(expected);
	}
}
