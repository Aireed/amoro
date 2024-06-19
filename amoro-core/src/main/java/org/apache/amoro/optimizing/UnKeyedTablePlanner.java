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

import org.apache.amoro.TableSnapshot;
import org.apache.amoro.data.PrimaryKeyedFile;
import org.apache.amoro.formats.mixed.MixedSnapshot;
import org.apache.amoro.scan.BasicMixedFileScanTask;
import org.apache.amoro.scan.MixedFileScanTask;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.TableProperties;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class UnKeyedTablePlanner extends OptimizingPlanner {
  private final TableFileScanHelper scanHelper;
  private final int lookBack;
  private final long openFileCost;
  private final long splitSize;
  private CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles;
  private final TableSnapshot tableSnapshot;
  protected Set<StructLike> baseStoreRewritePartition = Sets.newHashSet();
  private final Set<StructLike> allRewritePartition = Sets.newHashSet();
  private final PartitionSpec spec;
  protected MixedTable table;

  public UnKeyedTablePlanner(MixedTable table) {
    this.table = table;
    spec = table.spec();
    tableSnapshot = tableSnapshot();
    this.scanHelper =
        new UnkeyedTableFileScanHelper(table.asUnkeyedTable(), Long.valueOf(tableSnapshot.id()));
    openFileCost =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.SPLIT_OPEN_FILE_COST,
            TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    splitSize =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.SELF_OPTIMIZING_TARGET_SIZE,
            TableProperties.SELF_OPTIMIZING_TARGET_SIZE_DEFAULT);
    lookBack =
        PropertyUtil.propertyAsInt(
            table.properties(),
            TableProperties.SPLIT_LOOKBACK,
            TableProperties.SPLIT_LOOKBACK_DEFAULT);
  }

  public RewriteFilter rewriteFileFilter(
      CloseableIterable<TableFileScanHelper.FileScanResult> fileScanResults) {
    return new CommonRewriteFilter(table, fileScanResults);
  }

  @Override
  public CloseableIterable<CombinedScanTask> planFiles() {
    CloseableIterable<TableFileScanHelper.FileScanResult> fileScanResults =
        scanHelper.withPartitionFilter(filter).scan();

    RewriteFilter rewriteFileFilter = rewriteFileFilter(fileScanResults);
    baseStoreRewritePartition = rewriteFileFilter.needRewritePartition();
    rewriteFiles =
        CloseableIterable.filter(
            fileScanResults, (result) -> rewriteFileFilter.test(result.file()));
    rewriteFiles.forEach(
        fileScanResult -> allRewritePartition.add(fileScanResult.file().partition()));
    StructLikeMap<List<FileScanTask>> groupByPartition =
        groupByPartition(CloseableIterable.transform(rewriteFiles, this::buildArcticFileScanTask));

    return CloseableIterable.concat(
        groupByPartition.values().stream()
            .map(e -> split(CloseableIterable.withNoopClose(e)))
            .collect(Collectors.toList()));
  }

  @Override
  public CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles() {
    return rewriteFiles;
  }

  @Override
  public Set<StructLike> baseStoreRewritePartition() {
    return baseStoreRewritePartition;
  }

  @Override
  public Set<StructLike> allRewritePartition() {
    return allRewritePartition;
  }

  @Override
  public TableSnapshot tableSnapshot() {
    Snapshot changeSnapshot;
    Snapshot baseSnapshot;
    if (table.isKeyedTable()) {
      changeSnapshot = table.asKeyedTable().changeTable().currentSnapshot();
      baseSnapshot = table.asKeyedTable().baseTable().currentSnapshot();
    } else {
      changeSnapshot = null;
      baseSnapshot = table.asUnkeyedTable().currentSnapshot();
    }

    if (changeSnapshot == null && baseSnapshot == null) {
      return null;
    }

    return new MixedSnapshot(changeSnapshot, baseSnapshot);
  }

  public MixedFileScanTask buildArcticFileScanTask(
      TableFileScanHelper.FileScanResult fileScanResult) {
    PrimaryKeyedFile primaryKeyedFile = (PrimaryKeyedFile) fileScanResult.file();
    return new BasicMixedFileScanTask(
        primaryKeyedFile,
        fileScanResult.deleteFiles().stream()
            .filter(e -> e.content() == FileContent.POSITION_DELETES)
            .map(e -> (DeleteFile) e)
            .collect(Collectors.toList()),
        spec);
  }

  private StructLikeMap<List<FileScanTask>> groupByPartition(Iterable<FileScanTask> tasks) {
    Types.StructType partitionType = spec.partitionType();
    StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
    StructLike emptyStruct = GenericRecord.create(partitionType);

    for (FileScanTask task : tasks) {
      // If a task uses an incompatible partition spec the data inside could contain values
      // which belong to multiple partitions in the current spec. Treating all such files as
      // un-partitioned and grouping them together helps to minimize new files made.
      StructLike taskPartition =
          task.file().specId() == spec.specId() ? task.file().partition() : emptyStruct;

      List<FileScanTask> files = filesByPartition.get(taskPartition);
      if (files == null) {
        files = Lists.newArrayList();
      }

      files.add(task);
      filesByPartition.put(taskPartition, files);
    }
    return filesByPartition;
  }

  private CloseableIterable<CombinedScanTask> split(CloseableIterable<FileScanTask> tasks) {
    Function<FileScanTask, Long> weightFunc =
        task -> Math.max(task.file().fileSizeInBytes(), openFileCost);
    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(tasks, splitSize, lookBack, weightFunc, true), tasks),
        BaseCombinedScanTask::new);
  }
}
