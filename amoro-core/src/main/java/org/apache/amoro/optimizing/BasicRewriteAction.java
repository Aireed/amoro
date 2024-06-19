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

package org.apache.amoro.optimizing;

import org.apache.amoro.table.MixedTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeMap;

import java.util.List;
import java.util.UUID;

public abstract class BasicRewriteAction implements Action<RewriteResult> {

  protected final MixedTable table;
  protected Expression filter = Expressions.alwaysTrue();
  protected OptimizingPlanner planner;

  public BasicRewriteAction(MixedTable table) {
    this.table = table;
    this.planner = planner();
  }

  @Override
  public RewriteResult execute() {
    String groupId = UUID.randomUUID().toString();
    CloseableIterable<?> task = planner.scanFilter(filter).planFiles();
    List<?> tasks = Lists.newArrayList(task);
    if (tasks.isEmpty()) {
      return RewriteResult.empty();
    }
    ScanTaskSetManager.get().stageTasks(table, groupId, tasks);

    doRewrite(groupId);

    Table icebergTable =
        table.isKeyedTable() ? table.asKeyedTable().baseTable() : table.asUnkeyedTable();
    FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();
    RewriteCommitter committer = committer();
    committer.addFiles(coordinator.fetchNewDataFiles(icebergTable, groupId));
    committer.commitOrClean();
    return committer.result();
  }

  public abstract void doRewrite(String groupId);

  public OptimizingPlanner planner() {
    boolean isKeyedTable = table.isKeyedTable();
    if (isKeyedTable) {
      return new KeyedTablePlanner(table);
    } else {
      return new UnKeyedTablePlanner(table);
    }
  }

  public RewriteCommitter committer() {
    StructLikeMap<Long> lastPartitionSequence =
        table.isKeyedTable() ? ((KeyedTablePlanner) planner).lastPartitionSequence() : null;

    return new MixFormatRewriteCommitter(
        getTable(),
        planner.allRewritePartition(),
        planner.rewriteFiles(),
        Long.valueOf(planner.tableSnapshot().id()),
        lastPartitionSequence);
  }

  public BasicRewriteAction filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  protected MixedTable getTable() {
    return table;
  }
}
