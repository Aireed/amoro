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

package org.apache.amoro.spark.procedures;

import org.apache.amoro.api.BlockableOperation;
import org.apache.amoro.api.OperationConflictException;
import org.apache.amoro.mixed.MixedFormatCatalog;
import org.apache.amoro.optimizing.RewriteResult;
import org.apache.amoro.spark.actions.SparkRewriteAction;
import org.apache.amoro.spark.table.ArcticSparkTable;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.blocker.Blocker;
import org.apache.amoro.table.blocker.TableBlockerManager;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Predicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.execution.datasources.SparkExpressionConverter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RewriteDataFilesProcedure extends BaseProcedure {

  static final DataType STRING_MAP =
      DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("where", DataTypes.StringType),
        ProcedureParameter.optional("options", STRING_MAP)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(
                "rewritten_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "rewritten_delete_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "rewritten_partition_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "rewritten_base_store_partition_count",
                DataTypes.IntegerType,
                false,
                Metadata.empty()),
            new StructField("rewritten_partitions", DataTypes.StringType, false, Metadata.empty()),
            new StructField(
                "added_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
          });

  protected TableBlockerManager tableBlockerManager;
  protected Blocker block;

  public static SupportProcedures.ProcedureBuilder<RewriteDataFilesProcedure> builder() {
    return new SupportProcedures.ProcedureBuilder<RewriteDataFilesProcedure>() {
      TableCatalog tableCatalog;

      @Override
      public SupportProcedures.ProcedureBuilder<RewriteDataFilesProcedure> withTableCatalog(
          TableCatalog tableCatalog) {
        this.tableCatalog = tableCatalog;
        return this;
      }

      @Override
      public RewriteDataFilesProcedure build() {
        return new RewriteDataFilesProcedure(tableCatalog);
      }
    };
  }

  public RewriteDataFilesProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    ArcticSparkTable sparkTable =
        loadSparkTable(toIdentifier(args.getString(0), PARAMETERS[0].name()));
    MixedTable arcticTable = sparkTable.table();
    MixedFormatCatalog arcticCatalog = sparkTable.catalog();
    tableBlockerManager = arcticCatalog.getTableBlockerManager(arcticTable.id());
    if (conflict(tableBlockerManager.getBlockers())) {
      throw new IllegalStateException("table is blocked by optimize");
    }

    try {
      getBlocker(arcticCatalog, arcticTable);
      checkBlocker(tableBlockerManager);

      String where = args.isNullAt(1) ? null : args.getString(1);
      SparkRewriteAction action = actions().rewriteDataFiles(arcticTable);
      action = checkAndApplyFilter(action, where, arcticTable);
      return toOutputRows(action.execute());
    } finally {
      tableBlockerManager.release(block);
    }
  }

  private InternalRow[] toOutputRows(RewriteResult result) {
    InternalRow row =
        newInternalRow(
            result.rewriteFileCount(),
            result.rewriteDeleteFileCount(),
            result.rewritePartitionCount(),
            result.rewriteBaseStorePartitionCount(),
            UTF8String.fromString(result.rewritePartitions()),
            result.addFileCount());
    return new InternalRow[] {row};
  }

  private InternalRow newInternalRow(Object... values) {
    return new GenericInternalRow(values);
  }

  private boolean conflict(List<Blocker> blockers) {
    return blockers.stream()
        .anyMatch(blocker -> blocker.operations().contains(BlockableOperation.OPTIMIZE));
  }

  private void getBlocker(MixedFormatCatalog catalog, MixedTable table) {
    this.tableBlockerManager = catalog.getTableBlockerManager(table.id());
    ArrayList<BlockableOperation> operations = Lists.newArrayList();
    operations.add(BlockableOperation.ONLINE_OPTIMIZE);
    try {
      this.block = tableBlockerManager.block(operations);
    } catch (OperationConflictException e) {
      throw new IllegalStateException(
          "failed to block table " + table.id() + " with " + operations, e);
    }
  }

  private void checkBlocker(TableBlockerManager tableBlockerManager) {
    List<String> blockerIds =
        tableBlockerManager.getBlockers().stream()
            .map(Blocker::blockerId)
            .collect(Collectors.toList());
    if (!blockerIds.contains(block.blockerId())) {
      throw new IllegalStateException("block is not in blockerManager");
    }
  }

  private SparkRewriteAction checkAndApplyFilter(
      SparkRewriteAction action, String where, MixedTable table) {
    if (where != null) {
      try {
        Expression expression =
            SparkExpressionConverter.collectResolvedSparkExpression(
                spark,
                String.format("%s.%s", table.id().getDatabase(), table.id().getTableName()),
                where);
        org.apache.iceberg.expressions.Expression filter =
            SparkExpressionConverter.convertToIcebergExpression(expression);
        checkExpression(filter, table);
        return (SparkRewriteAction) action.filter(filter);
      } catch (AnalysisException e) {
        throw new IllegalArgumentException("Cannot parse predicates in where option: " + where, e);
      }
    }
    return action;
  }

  public void checkExpression(org.apache.iceberg.expressions.Expression expr, MixedTable table) {
    if (table.isUnkeyedTable()) {
      return;
    }
    List<String> partitionNames =
        table.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
    if (expr instanceof Predicate) {
      if (expr instanceof BoundPredicate) {
        if (!partitionNames.contains(((BoundPredicate<?>) expr).ref().field().name())) {
          throw new UnsupportedOperationException(
              "keyed table only support partition filter, but got field "
                  + ((BoundPredicate<?>) expr).ref().field().name()
                  + " is not partition field");
        }
      } else {
        if (!partitionNames.contains(((UnboundPredicate<?>) expr).ref().name())) {
          throw new UnsupportedOperationException(
              "keyed table only support partition filter, but got field "
                  + ((UnboundPredicate<?>) expr).ref().name()
                  + " is not partition field");
        }
      }
    }
  }

  @Override
  public String description() {
    return "RewriteDataFilesProcedure";
  }
}
