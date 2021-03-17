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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Static;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

/** Extends Calcite's {@link SqlValidator} by Flink-specific behavior. */
@Internal
public final class FlinkCalciteSqlValidator extends SqlValidatorImpl {

    public FlinkCalciteSqlValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlValidator.Config config) {
        super(opTab, catalogReader, typeFactory, config);
    }

    @Override
    public void validateLiteral(SqlLiteral literal) {
        if (literal.getTypeName() == DECIMAL) {
            final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
            if (decimal.precision() > DecimalType.MAX_PRECISION) {
                throw newValidationError(
                        literal, Static.RESOURCE.numberLiteralOutOfRange(decimal.toString()));
            }
        }
        super.validateLiteral(literal);
    }

    @Override
    protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        // Due to the improper translation of lateral table left outer join in Calcite, we need to
        // temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
        if (join.getJoinType() == JoinType.LEFT
                && SqlUtil.stripAs(join.getRight()).getKind() == SqlKind.COLLECTION_TABLE) {
            SqlNode right = SqlUtil.stripAs(join.getRight());
            if (right instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) right;
                SqlNode operand0 = call.operand(0);
                if (operand0 instanceof SqlBasicCall
                        && ((SqlBasicCall) operand0).getOperator()
                                instanceof SqlWindowTableFunction) {
                    return;
                }
            }
            final SqlNode condition = join.getCondition();
            if (condition != null
                    && (!SqlUtil.isLiteral(condition)
                            || ((SqlLiteral) condition).getValueAs(Boolean.class)
                                    != Boolean.TRUE)) {
                throw new ValidationException(
                        String.format(
                                "Left outer joins with a table function do not accept a predicate such as %s. "
                                        + "Only literal TRUE is accepted.",
                                condition));
            }
        }
        super.validateJoin(join, scope);
    }

    @Override
    public void validateColumnListParams(
            SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
        // we don't support column lists and translate them into the unknown type in the type
        // factory,
        // this makes it possible to ignore them in the validator and fall back to regular row types
        // see also SqlFunction#deriveType
    }

    //    @Override
    //    public void validateQuery(SqlNode node, SqlValidatorScope scope, RelDataType
    // targetRowType) {
    //        if (node.getKind() == SqlKind.SNAPSHOT) {
    //            final SqlValidatorNamespace ns = getNamespace(node);
    //            validateNamespace(ns, targetRowType);
    //            validateSnapshot(node, scope, ns);
    //        } else {
    //            super.validateQuery(node, scope, targetRowType);
    //        }
    //    }
    //
    //    /**
    //     * Validates snapshot to a table.
    //     *
    //     * <p>Flink enables TIMESTAMP and TIMESTAMP_LTZ for system time period specification type.
    //     *
    //     * @param node The node to validate
    //     * @param scope Validator scope to derive type
    //     * @param ns The namespace to lookup table
    //     */
    //    private void validateSnapshot(SqlNode node, SqlValidatorScope scope, SqlValidatorNamespace
    // ns) {
    //        if (node.getKind() == SqlKind.SNAPSHOT) {
    //            SqlSnapshot snapshot = (SqlSnapshot) node;
    //            SqlNode period = snapshot.getPeriod();
    //            RelDataType dataType = deriveType(scope, period);
    //            if (!(dataType.getSqlTypeName() == SqlTypeName.TIMESTAMP
    //                    || dataType.getSqlTypeName() ==
    // SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
    //                throw newValidationError(
    //                        period,
    //                        Static.RESOURCE.illegalExpressionForTemporal(
    //                                dataType.getSqlTypeName().getName()));
    //            }
    //            if (!ns.getTable().isTemporal()) {
    //                List<String> qualifiedName = ns.getTable().getQualifiedName();
    //                String tableName = qualifiedName.get(qualifiedName.size() - 1);
    //                throw newValidationError(
    //                        snapshot.getTableRef(), Static.RESOURCE.notTemporalTable(tableName));
    //            }
    //        }
    //    }
}
