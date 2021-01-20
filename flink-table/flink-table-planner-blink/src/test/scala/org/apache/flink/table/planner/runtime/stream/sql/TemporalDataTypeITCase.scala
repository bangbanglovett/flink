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

import java.time.ZoneId
import java.util
import java.util.TimeZone

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ExplainDetail
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestData, TestingAppendSink, TestingRetractSink}
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

class TemporalDataTypeITCase extends StreamingTestBase {
  @Before
  override def before(): Unit = {
    super.before()
    val myTableDataId = TestValuesTableFactory.registerData(TestData.smallData3)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyTable (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
//         |  ,
//         |  proctime as PROCTIME(),
//         |  rowtime as TIMESTAMP '1970-01-01 00:00:44',
//         |  WATERMARK FOR rowtime as rowtime
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$myTableDataId',
         |  'bounded' = 'false'
         |)
         |""".stripMargin)
  }

  @Test
  def testSimpleNoTimezoneTemporalDataType(): Unit = {
    val columns = new util.ArrayList[java.lang.String]()
    columns.add("DATE '1970-01-01'")
    columns.add("TIME '00:00:44'")
    columns.add("TIMESTAMP '1970-01-01 00:00:44'")
    columns.add("INTERVAL '1 00:00:01.234' DAY TO SECOND")
    columns.add("YEAR(DATE '1970-01-01')")
    columns.add("QUARTER(DATE '1970-01-01')")
    columns.add("MONTH(DATE '1970-01-01')")
    columns.add("WEEK(DATE '1970-01-01')")
    columns.add("DAYOFYEAR(DATE '1970-01-01')")
    columns.add("DAYOFMONTH(DATE '1970-01-01')")
    columns.add("DAYOFWEEK(DATE '1970-01-01')")
    columns.add("HOUR(TIMESTAMP '1970-01-01 00:00:44')")
    columns.add("MINUTE(TIMESTAMP '1970-01-01 00:00:44')")
    columns.add("SECOND(TIMESTAMP '1970-01-01 00:00:44')")
    columns.add("FLOOR(TIME '12:44:31' TO MINUTE)")
    columns.add("CEIL(TIME '12:44:31' TO MINUTE)")
    columns.add("(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '1:56:00', INTERVAL '1' HOUR)")
    columns.add("(TIME '9:00:00', TIME '10:00:00') OVERLAPS (TIME '10:15:00', INTERVAL '3' HOUR)")
    columns.add("TIMESTAMPADD(WEEK, 1, DATE '2003-01-02')")
    columns.add("TIMESTAMPDIFF(DAY, TIMESTAMP '2003-01-02 10:00:00'," +
      " TIMESTAMP '2003-01-03 10:00:00')")
    testDataType(columns)
    println()
    println()
    testDataType(castToString(columns))
  }

  @Test
  def testSimpleNoTimezoneTemporalFunctions(): Unit = {
    val columns = new util.ArrayList[java.lang.String]()
    columns.add("a")
    columns.add("CONVERT_TZ('1970-01-01 00:00:44','Asia/Shanghai', 'UTC')")
    columns.add("NOW()")
    columns.add("PROCTIME()")
    columns.add("PROCTIME_MATERIALIZE(PROCTIME())")
    columns.add("PROCTIME_MATERIALIZE(TIMESTAMP '1970-01-01 00:00:44')")
    columns.add("TO_DATE('1970-01-01')")
    columns.add("TO_DATE('1970/01/01', 'yyyy/MM/dd')")
    columns.add("DATE_FORMAT(TIMESTAMP '1970-01-01 00:00:44', 'yyyy/MM/dd')")
    columns.add("DATE_FORMAT(TIMESTAMP '1970-01-01 00:00:44', 'yyyy-MM-dd')")
    testDataType(columns)
    println()
    println()
    testDataType(castToString(columns))
  }

  @Test
  def testSimpleTimezoneTemporalFunctions(): Unit = {
    val columns = new util.ArrayList[java.lang.String]()
    columns.add("UNIX_TIMESTAMP('1970/01/01 00:00:44','yyyy/MM/dd HH:mm:ss')")
    columns.add("unix_timestamp('2009-03-20 11:30:01','yyyy-MM-dd HH:mm:ss')")
    columns.add("CURRENT_DATE")
    columns.add("CURRENT_TIME")
    columns.add("LOCALTIME")
    columns.add("CURRENT_TIMESTAMP")
    columns.add("LOCALTIMESTAMP")
    columns.add("NOW()")
    columns.add("PROCTIME()")
    columns.add("UNIX_TIMESTAMP()")
    columns.add("UNIX_TIMESTAMP('1970/01/01 00:00:44','yyyy/MM/dd HH:mm:ss')")
    columns.add("FROM_UNIXTIME(44, 'yyyy-MM-dd HH:mm:ss')")
    columns.add("FROM_UNIXTIME(1607391742, 'yyyy-MM-dd HH:mm:ss')")
    columns.add("FROM_UNIXTIME(1607362942, 'yyyy-MM-dd HH:mm:ss')")
    columns.add("FROM_UNIXTIME(UNIX_TIMESTAMP())")
    columns.add("TO_TIMESTAMP('1970-01-01 00:00:44')")
    columns.add("TO_TIMESTAMP('1970/01/01 00:00:44.123', 'yyyy/MM/dd HH:mm:ss.SSS')")
    testDataType(columns)
    println()
    println()
    testDataType(castToString(columns))
  }

  @Test
  def testProctime(): Unit = {
    val columns = new util.ArrayList[java.lang.String]()
    columns.add("CAST(44 AS TIMESTAMP)")
    columns.add("CAST(timestamp '1970-01-01 00:00:44' as BIGINT)")


    columns.add("CURRENT_TIMESTAMP")
    columns.add("LOCALTIMESTAMP")
    columns.add("PROCTIME()")
    columns.add("CAST(PROCTIME() AS STRING)")

    testDataType(columns)
    testDataType(castToString(columns))
  }

  @Test
  def testTemporalDataTypeCast(): Unit = {
    val columns = new util.ArrayList[java.lang.String]()
    columns.add("TIME '00:00:44'")
    columns.add("TIMESTAMP '1970-01-01 00:00:44'")
    columns.add("TO_TIMESTAMP('1970-01-01 00:00:44')")
    columns.add("CAST(TO_TIMESTAMP('1970-01-01 00:00:44') AS TIMESTAMP" +
      " WITH LOCAL TIME ZONE)")
    columns.add("CAST(CAST(TIMESTAMP '1970-01-01 00:00:44' AS TIMESTAMP" +
      " WITH LOCAL TIME ZONE) AS TIMESTAMP)")

    columns.add(" CAST(TIMESTAMP '1970-01-01 08:00:44' AS TIMESTAMP" +
      " WITH LOCAL TIME ZONE) ")
    columns.add("TO_TIMESTAMP('1970-01-01 00:00:44') + INTERVAL '1' DAY")

    testDataType(columns)
    testDataType(castToString(columns))
  }

  @Test
  def testWindowTemporalFunction(): Unit = {
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))
    tEnv.executeSql("SELECT a," +
      " CAST(TUMBLE_PROCTIME(proctime, INTERVAL '1' SECOND) AS STRING)," +
      " CAST(TUMBLE_START(proctime, INTERVAL '1' SECOND) AS STRING)," +
      " CAST(TUMBLE_END(proctime, INTERVAL '1' SECOND) AS STRING)" +
      " FROM MyTable" +
      " GROUP BY TUMBLE(proctime, INTERVAL '1' SECOND), a")
      .print()
    println()
    println()
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    tEnv.executeSql("SELECT a," +
      " CAST(TUMBLE_PROCTIME(proctime, INTERVAL '1' SECOND) AS STRING)," +
      " CAST(TUMBLE_START(proctime, INTERVAL '1' SECOND) AS STRING)," +
      " CAST(TUMBLE_END(proctime, INTERVAL '1' SECOND) AS STRING)" +
      " FROM MyTable" +
      " GROUP BY TUMBLE(proctime, INTERVAL '1' SECOND), a")
      .print()
  }


  //--------

  private def castToString(columns: util.List[String]): util.List[String] = {
    columns.map(col => "CAST(" + col + " AS STRING)")
      .toList
  }

  private def testDataType(columns: util.List[String]): Unit = {
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))
    println("Current session timezone: " + tEnv.getConfig.getLocalTimeZone)
    val table1 = tEnv.sqlQuery("SELECT " + columns.mkString(",") + " FROM MyTable")
    val result1 = table1.toAppendStream[Row]
    val sink1 = new TestingAppendSink(TimeZone.getTimeZone(tEnv.getConfig.getLocalTimeZone))
    result1.addSink(sink1)
    env.execute()
    printFormattedRes(
      columns,
      table1.getSchema.getFieldDataTypes.map(t => t.toString).toList,
      sink1.getAppendResults.head.split(",").toList)
//    printFormattedRes(
//      columns,
//      table1.getSchema.getFieldDataTypes.map(t => t.toString).toList,
//      sink1.getAppendResults.last.split(",").toList)


    println()
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    println("Current session timezone: " + tEnv.getConfig.getLocalTimeZone)
    val table2 = tEnv.sqlQuery("SELECT " + columns.mkString(",") + " FROM MyTable")
    val result = table2.toAppendStream[Row]
    val sink2 = new TestingAppendSink(TimeZone.getTimeZone(tEnv.getConfig.getLocalTimeZone))
    result.addSink(sink2)
    env.execute()
    printFormattedRes(
      columns,
      table2.getSchema.getFieldDataTypes.map(t => t.toString).toList,
      sink2.getAppendResults.head.split(",").toList)
//    printFormattedRes(
//      columns,
//      table2.getSchema.getFieldDataTypes.map(t => t.toString).toList,
//      sink2.getAppendResults.last.split(",").toList)
  }

  private def printFormattedRes(
                                 columns: util.List[String],
                                 types: util.List[String],
                                 results: util.List[String]): Unit = {

    Preconditions.checkArgument(columns.length == types.length && types.length == results.length)
    val fixedLenArr = columns.zipWithIndex.map(c => {
      Math.max(Math.max(c._1.length, types.get(c._2).length), results.get(c._2).length)
    }).toList

    printData(columns, fixedLenArr)
    printData(types, fixedLenArr)
    printData(results, fixedLenArr)
  }

  private def printData(data: util.List[String], fixedLenArr: util.List[Int]): Unit = {
    println("|  " +
      data.zip(fixedLenArr).map(s => formatStrWithFixedLen(s._1, s._2)).mkString("  | ") + "  |")
  }

  private def formatStrWithFixedLen(string: String, fixedLen: Int): String = {
    if (string.length < fixedLen) {
      val sb = new StringBuilder
      sb.append(string)
      var i = fixedLen - string.length
      while (i > 0) {
        sb.append(" ")
        i -= 1
      }
      sb.mkString("")
    } else {
      string.substring(0, Math.min(string.length, fixedLen))
    }
  }

}
