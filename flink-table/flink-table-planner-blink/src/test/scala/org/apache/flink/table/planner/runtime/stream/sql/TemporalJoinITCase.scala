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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase
import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.lang.{Long => JLong}

import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.table.planner.factories.TestValuesTableFactory.getRawResults
import org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData


@RunWith(classOf[Parameterized])
class TemporalJoinITCase(state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  // test data for Processing-Time temporal table join
  val procTimeOrderData = List(
    changelogRow("+I", toJLong(1), "Euro", "no1", toJLong(12)),
    changelogRow("+I", toJLong(2), "US Dollar", "no1", toJLong(14)),
    changelogRow("+I", toJLong(3), "US Dollar", "no2", toJLong(18)),
    changelogRow("+I", toJLong(4), "RMB", "no1", toJLong(40)),
    // simply test left stream could be changelog,
    // -U or -D message may retract fail in COLLECTION sink check
    changelogRow("+U", toJLong(4), "RMB", "no1", toJLong(60)))

  val procTimeCurrencyData = List(
    changelogRow("+I","Euro", "no1", toJLong(114)),
    changelogRow("+I","US Dollar", "no1", toJLong(102)),
    changelogRow("+I","Yen", "no1", toJLong(1)),
    changelogRow("+I","RMB", "no1", toJLong(702)),
    changelogRow("+I","Euro", "no1", toJLong(118)),
    changelogRow("+I","US Dollar", "no2", toJLong(106)))

  @Before
  def prepare(): Unit = {
    env.setParallelism(1)
    val procTimeOrderDataId = registerData(procTimeOrderData)

    tEnv.executeSql(
      s"""
         |CREATE TABLE orders_proctime (
         |  order_id BIGINT,
         |  currency STRING,
         |  currency_no STRING,
         |  amount BIGINT,
         |  proctime as PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'data-id' = '$procTimeOrderDataId'
         |)
         |""".stripMargin)

    // register a non-lookup table
    val procTimeCurrencyDataId = registerData(procTimeCurrencyData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE currency_proctime (
         |  currency STRING,
         |  currency_no STRING,
         |  rate BIGINT,
         |  proctime as PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'disable-lookup' = 'true',
         |  'data-id' = '$procTimeCurrencyDataId'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE TABLE changelog_currency_proctime (
         |  currency STRING,
         |  currency_no STRING,
         |  rate BIGINT,
         |  proctime as PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'disable-lookup' = 'true',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'data-id' = '$procTimeCurrencyDataId'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE VIEW latest_rates AS
         |SELECT
         |  currency,
         |  currency_no,
         |  rate,
         |  proctime FROM
         |      ( SELECT *, ROW_NUMBER() OVER (PARTITION BY currency, currency_no
         |        ORDER BY proctime DESC) AS rowNum
         |        FROM currency_proctime) T
         | WHERE rowNum = 1""".stripMargin)
  }

  /**
   * Because of nature of the processing time, we can not (or at least it is not that easy)
   * validate the result here. Instead of that, here we are just testing whether there are no
   * exceptions in a full blown ITCase. Actual correctness is tested in unit tests.
   */
  @Test
  def testProcTimeTemporalJoin(): Unit = {
    createSinkTable("proctime_sink1", None)
    val sql = "INSERT INTO proctime_sink1 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"
    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeLeftTemporalJoin(): Unit = {
    createSinkTable("proctime_sink2", None)
    val sql = "INSERT INTO proctime_sink2 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    tEnv.executeSql(sql).await()
    val rawResult = getRawResults("proctime_sink2")
    assertEquals(procTimeOrderData.size, rawResult.size())
  }

  @Test
  def testProcTimeTemporalJoinChangelogSource(): Unit = {
    createSinkTable("proctime_sink3", Some(
      s"""
      | currency STRING,
      | currency_no STRING,
      | rate BIGINT,
      | proctime TIMESTAMP(3)
      | """.stripMargin))

    val sql = "INSERT INTO proctime_sink3 " +
      " SELECT r.* FROM orders_proctime AS o " +
      " JOIN changelog_currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeTemporalJoinWithView(): Unit = {
    createSinkTable("proctime_sink4", None)
    val sql = "INSERT INTO proctime_sink4 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeLeftTemporalJoinWithView(): Unit = {
    createSinkTable("proctime_sink5", None)
    val sql = "INSERT INTO proctime_sink5 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    tEnv.executeSql(sql).await()
    val rawResult = getRawResults("proctime_sink5")
    assertEquals(procTimeOrderData.size, rawResult.size())
  }

  @Test
  def testProcTimeTemporalJoinWithViewNonEqui(): Unit = {
    createSinkTable("proctime_sink6", None)
    val sql = "INSERT INTO proctime_sink6 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime AS r " +
      " ON o.currency = r.currency AND o.amount > r.rate"

    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeLeftTemporalJoinWithViewWithPredicates(): Unit = {
    createSinkTable("proctime_sink7", None)
    val sql = "INSERT INTO proctime_sink7 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime AS r " +
      " ON o.currency = r.currency AND o.amount > r.rate"

    tEnv.executeSql(sql).await()
    val rawResult = getRawResults("proctime_sink7")
    assertEquals(procTimeOrderData.size, rawResult.size())
  }

  @Test
  def testProcTimeMultiTemporalJoin(): Unit = {
    createSinkTable("proctime_sink8", None)
    val sql = "INSERT INTO proctime_sink8 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r1.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r1" +
      " ON o.currency = r1.currency and o.currency_no = r1.currency_no"

    tEnv.executeSql(sql).await()
  }

  private def createSinkTable(tableName: String, columns: Option[String]): Unit = {
    val columnsDDL = columns match {
      case Some(cols) => cols
      case _ =>
        s"""
           |  order_id BIGINT,
           |  currency STRING,
           |  amount BIGINT,
           |  l_time TIMESTAMP(3),
           |  rate BIGINT,
           |  r_time TIMESTAMP(3)
           |""".stripMargin
    }

    tEnv.executeSql(
      s"""
        |CREATE TABLE $tableName (
        | $columnsDDL
        |) WITH (
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false',
        |  'changelog-mode' = 'I,UA,UB,D'
        |)
        |""".stripMargin)
  }

  private def toJLong(int: Int): JLong = {
    JLong.valueOf(int)
  }
}
