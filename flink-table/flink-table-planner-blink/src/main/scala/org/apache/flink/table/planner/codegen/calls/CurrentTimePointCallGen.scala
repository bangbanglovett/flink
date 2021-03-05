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

package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.planner.codegen.GenerateUtils.generateNonNullField
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot.{DATE, TIMESTAMP_WITHOUT_TIME_ZONE,TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIME_WITHOUT_TIME_ZONE}

/**
  * Generates function call to determine current time point (as date/time/timestamp) in
  * local timezone or not.
  */
class CurrentTimePointCallGen(local: Boolean, fallbackLegacyImpl: Boolean = false, isRowLevel: Boolean = true) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = returnType.getTypeRoot match {
    case TIME_WITHOUT_TIME_ZONE if local && isRowLevel =>
      val time = ctx.addReusableLocalTime()
      generateNonNullField(returnType, time)

    case TIME_WITHOUT_TIME_ZONE if local && !isRowLevel =>
      val time = ctx.addReusableSessionLocalTime()
      generateNonNullField(returnType, time)

    case TIMESTAMP_WITHOUT_TIME_ZONE if local && isRowLevel =>
      val timestamp = ctx.addReusableLocalDateTime()
      generateNonNullField(returnType, timestamp)

    case TIMESTAMP_WITHOUT_TIME_ZONE if local && !isRowLevel =>
      val timestamp = ctx.addReusableSessionLocalDateTime()
      generateNonNullField(returnType, timestamp)

    case DATE if fallbackLegacyImpl && isRowLevel =>
      val date = ctx.addReusableDate()
      generateNonNullField(returnType, date)

    case DATE if fallbackLegacyImpl && !isRowLevel =>
      val date = ctx.addReusableSessionDate()
      generateNonNullField(returnType, date)

    case TIME_WITHOUT_TIME_ZONE if fallbackLegacyImpl && isRowLevel =>
      val time = ctx.addReusableTime()
      generateNonNullField(returnType, time)

    case TIME_WITHOUT_TIME_ZONE if fallbackLegacyImpl && !isRowLevel =>
      val time = ctx.addReusableSessionTime()
      generateNonNullField(returnType, time)

    case TIMESTAMP_WITHOUT_TIME_ZONE if fallbackLegacyImpl && isRowLevel =>
      val timestamp = ctx.addReusableTimestamp()
      generateNonNullField(returnType, timestamp)

    case TIMESTAMP_WITHOUT_TIME_ZONE if fallbackLegacyImpl && !isRowLevel =>
      val timestamp = ctx.addReusableSessionTimestamp()
      generateNonNullField(returnType, timestamp)

    case DATE if !fallbackLegacyImpl && isRowLevel =>
      val date = ctx.addReusableCurrentDate()
      generateNonNullField(returnType, date)

    case DATE if !fallbackLegacyImpl && !isRowLevel =>
      val date = ctx.addReusableSessionCurrentDate()
      generateNonNullField(returnType, date)

    case TIME_WITHOUT_TIME_ZONE if !fallbackLegacyImpl && isRowLevel =>
      val time = ctx.addReusableCurrentTime()
      generateNonNullField(returnType, time)

    case TIME_WITHOUT_TIME_ZONE if !fallbackLegacyImpl && !isRowLevel =>
      val time = ctx.addReusableSessionCurrentTime()
      generateNonNullField(returnType, time)

    case TIMESTAMP_WITH_LOCAL_TIME_ZONE if !fallbackLegacyImpl && isRowLevel =>
      val timestamp = ctx.addReusableTimestamp()
      generateNonNullField(returnType, timestamp)

    case TIMESTAMP_WITH_LOCAL_TIME_ZONE if !fallbackLegacyImpl && !isRowLevel =>
      val timestamp = ctx.addReusableSessionTimestamp()
      generateNonNullField(returnType, timestamp)
  }

}
