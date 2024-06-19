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

package org.apache.amoro.hive.optimizing;

import org.apache.amoro.formats.mixed.MixedSnapshot;
import org.apache.amoro.hive.utils.TableTypeUtil;
import org.apache.amoro.optimizing.BasicRewriteAction;
import org.apache.amoro.optimizing.KeyedTablePlanner;
import org.apache.amoro.optimizing.MixFormatRewriteCommitter;
import org.apache.amoro.optimizing.OptimizingPlanner;
import org.apache.amoro.optimizing.RewriteCommitter;
import org.apache.amoro.optimizing.UnKeyedTablePlanner;
import org.apache.amoro.table.MixedTable;
import org.apache.iceberg.util.StructLikeMap;

public abstract class SupportHiveRewriteAction extends BasicRewriteAction {

  public SupportHiveRewriteAction(MixedTable table) {
    super(table);
  }

  public OptimizingPlanner planner() {
    boolean isKeyedTable = table.isKeyedTable();
    boolean isHiveTable = TableTypeUtil.isHive(table);
    if (isKeyedTable) {
      if (isHiveTable) {
        return new KeyedHiveTablePlanner(table);
      } else {
        return new KeyedTablePlanner(table);
      }
    } else {
      if (isHiveTable) {
        return new UnKeyedHiveTablePlanner(table);
      } else {
        return new UnKeyedTablePlanner(table);
      }
    }
  }

  public RewriteCommitter committer() {
    StructLikeMap<Long> lastPartitionSequence =
        table.isKeyedTable() ? ((KeyedTablePlanner) planner).lastPartitionSequence() : null;

    MixedSnapshot snapshot = (MixedSnapshot) planner.tableSnapshot();
    Long snapshotId = snapshot.getBaseSnapshotId();
    if (TableTypeUtil.isHive(getTable())) {
      return new SupportHiveRewriteCommitter(
          getTable(),
          planner.baseStoreRewritePartition(),
          planner.allRewritePartition(),
          planner.rewriteFiles(),
          snapshotId,
          lastPartitionSequence);
    } else {
      return new MixFormatRewriteCommitter(
          getTable(),
          planner.allRewritePartition(),
          planner.rewriteFiles(),
          snapshotId,
          lastPartitionSequence);
    }
  }
}
