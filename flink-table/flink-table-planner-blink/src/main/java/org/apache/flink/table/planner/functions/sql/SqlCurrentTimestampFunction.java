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

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_FALLBACK_LEGACY_TIME_FUNCTION;

/**
 * Function that returns current timestamp, the function return type
 * is {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE}.
 *
 * NOTE: To be compatible with legacy behavior, the function return
 * type could be {@link SqlTypeName#TIMESTAMP} when enabled
 * {@link ExecutionConfigOptions#TABLE_EXEC_FALLBACK_LEGACY_TIME_FUNCTION}.
 */
public class SqlCurrentTimestampFunction extends SqlFunction {

    public SqlCurrentTimestampFunction(
            String name,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker) {
        super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, SqlFunctionCategory.TIMEDATE);
    }
    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION_ID;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        int precision = 0;
        if (opBinding.getOperandCount() == 1) {
            RelDataType type = opBinding.getOperandType(0);
            if (SqlTypeUtil.isNumeric(type)) {
                precision = opBinding.getOperandLiteralValue(0, Integer.class);
            }
        }
        assert precision >= 0;
        if (precision > SqlTypeName.MAX_DATETIME_PRECISION) {
            throw opBinding.newError(
                RESOURCE.argumentMustBeValidPrecision(
                    opBinding.getOperator().getName(),
                    0,
                    SqlTypeName.MAX_DATETIME_PRECISION));
        }
        TableConfig config =
            ((FlinkTypeFactory) opBinding.getTypeFactory()).getConfig();
        boolean fallbackLegacyImpl = config.getConfiguration().getBoolean(TABLE_EXEC_FALLBACK_LEGACY_TIME_FUNCTION);
        if (fallbackLegacyImpl) {
            return opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, precision);
        }
        return opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, precision);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return SqlMonotonicity.INCREASING;
    }
}
