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
import org.apache.amoro.data.DataTreeNode;
import org.apache.amoro.data.PrimaryKeyedFile;
import org.apache.amoro.formats.mixed.MixedSnapshot;
import org.apache.amoro.scan.BaseCombinedScanTask;
import org.apache.amoro.scan.BasicMixedFileScanTask;
import org.apache.amoro.scan.CombinedScanTask;
import org.apache.amoro.scan.MixedFileScanTask;
import org.apache.amoro.scan.NodeFileScanTask;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.utils.TablePropertyUtil;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyedTablePlanner extends OptimizingPlanner {
  private static Logger LOG = LoggerFactory.getLogger("KeyedTablePlanner");

  private final TableFileScanHelper scanHelper;
  private final int lookBack;
  private final long openFileCost;
  private final long splitSize;
  private final PartitionSpec spec;

  protected CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles;
  protected final TableSnapshot tableSnapshot;
  protected Set<StructLike> needRewritePartition = Sets.newHashSet();
  protected Set<StructLike> allRewritePartition = Sets.newHashSet();
  protected MixedTable table;
  protected final StructLikeMap<Long> lastPartitionSequence;

  public KeyedTablePlanner(MixedTable table) {
    this.table = table;
    spec = table.spec();
    tableSnapshot = tableSnapshot();

    this.scanHelper =
        new KeyedTableFileScanHelper(table.asKeyedTable(), ((MixedSnapshot) tableSnapshot));
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
    lastPartitionSequence =
        table.isKeyedTable()
            ? TablePropertyUtil.getPartitionOptimizedSequence(table.asKeyedTable())
            : null;
  }

  public RewriteFilter rewriteFileFilter(
      CloseableIterable<TableFileScanHelper.FileScanResult> fileScanResults) {
    return new CommonRewriteFilter(table, fileScanResults);
  }

  @Override
  public CloseableIterable<CombinedScanTask> planFiles() {
    CloseableIterable<TableFileScanHelper.FileScanResult> fileScanResultsIter =
        scanHelper.withPartitionFilter(filter).scan();
    List<TableFileScanHelper.FileScanResult> scanResultList = new ArrayList<>();
    // general a new iterable
    fileScanResultsIter.forEach(item -> scanResultList.add(item));
    CloseableIterable<TableFileScanHelper.FileScanResult> fileScanResults =
        CloseableIterable.withNoopClose(scanResultList);

    RewriteFilter rewriteFileFilter = rewriteFileFilter(fileScanResults);
    needRewritePartition = rewriteFileFilter.needRewritePartition();

    rewriteFiles =
        CloseableIterable.filter(
            fileScanResults, (result) -> rewriteFileFilter.test(result.file()));

    rewriteFiles.forEach(
        fileScanResult -> allRewritePartition.add(fileScanResult.file().partition()));

    Map<StructLike, Collection<TableFileScanHelper.FileScanResult>> partitionFiles =
        groupFilesByPartition(rewriteFiles);

    Map<StructLike, List<NodeFileScanTask>> fileScanTasks =
        partitionFiles.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> partitionNodeTask(e.getValue())));

    return combineNode(
        CloseableIterable.withNoopClose(split(fileScanTasks)), splitSize, lookBack, openFileCost);
  }

  public CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles() {
    return rewriteFiles;
  }

  public Set<StructLike> baseStoreRewritePartition() {
    return needRewritePartition;
  }

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

  public StructLikeMap<Long> lastPartitionSequence() {
    return lastPartitionSequence;
  }

  public Map<StructLike, Collection<TableFileScanHelper.FileScanResult>> groupFilesByPartition(
      CloseableIterable<TableFileScanHelper.FileScanResult> fileScanResults) {
    ListMultimap<StructLike, TableFileScanHelper.FileScanResult> filesGroupedByPartition =
        Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
    fileScanResults.forEach(task -> filesGroupedByPartition.put(task.file().partition(), task));
    return filesGroupedByPartition.asMap();
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

  private List<NodeFileScanTask> partitionNodeTask(
      Collection<TableFileScanHelper.FileScanResult> partitionFiles) {
    Map<DataTreeNode, NodeFileScanTask> nodeFileScanTaskMap = new HashMap<>();
    // planfiles() cannot guarantee the uniqueness of the file,
    // so Set<path> here is used to remove duplicate files
    Set<String> pathSets = new HashSet<>();
    partitionFiles.forEach(
        partitionFile -> {
          PrimaryKeyedFile primaryKeyedFile = (PrimaryKeyedFile) partitionFile.file();
          if (!pathSets.contains(partitionFile.file().path().toString())) {
            pathSets.add(partitionFile.file().path().toString());
            DataTreeNode treeNode = primaryKeyedFile.node();
            NodeFileScanTask nodeFileScanTask =
                nodeFileScanTaskMap.getOrDefault(treeNode, new NodeFileScanTask(treeNode));
            MixedFileScanTask task = buildArcticFileScanTask(partitionFile);
            nodeFileScanTask.addFile(task);
            nodeFileScanTaskMap.put(treeNode, nodeFileScanTask);
          }
          partitionFile.deleteFiles().stream()
              .filter(e -> e.content() != FileContent.POSITION_DELETES)
              .forEach(
                  e -> {
                    PrimaryKeyedFile eqDeleteFile = (PrimaryKeyedFile) e;
                    if (!pathSets.contains(eqDeleteFile.path().toString())) {
                      pathSets.add(eqDeleteFile.path().toString());
                      DataTreeNode treeNode = eqDeleteFile.node();
                      NodeFileScanTask nodeFileScanTask =
                          nodeFileScanTaskMap.getOrDefault(
                              treeNode, new NodeFileScanTask(treeNode));
                      MixedFileScanTask task =
                          new BasicMixedFileScanTask(eqDeleteFile, Lists.newArrayList(), spec);
                      nodeFileScanTask.addFile(task);
                      nodeFileScanTaskMap.put(treeNode, nodeFileScanTask);
                    }
                  });
        });

    List<NodeFileScanTask> fileScanTaskList = new ArrayList<>();
    nodeFileScanTaskMap.forEach(
        (treeNode, nodeFileScanTask) -> {
          if (!nodeFileScanTask.isDataNode()) {
            return;
          }
          fileScanTaskList.add(nodeFileScanTask);
        });

    return fileScanTaskList;
  }

  private List<NodeFileScanTask> split(Map<StructLike, List<NodeFileScanTask>> fileScanTasks) {
    List<NodeFileScanTask> splitTasks = new ArrayList<>();
    fileScanTasks.forEach(
        (structLike, fileScanTasks1) -> {
          for (NodeFileScanTask task : fileScanTasks1) {
            if (task.cost() <= splitSize) {
              splitTasks.add(task);
              continue;
            }
            if (task.dataTasks().size() < 2) {
              splitTasks.add(task);
              continue;
            }
            CloseableIterable<NodeFileScanTask> tasksIterable =
                splitNode(
                    CloseableIterable.withNoopClose(task.dataTasks()),
                    task.mixedEquityDeletes(),
                    splitSize,
                    lookBack,
                    openFileCost);
            List<NodeFileScanTask> tasks = Lists.newArrayList(tasksIterable);
            splitTasks.addAll(tasks);
          }
        });
    return splitTasks;
  }

  public CloseableIterable<NodeFileScanTask> splitNode(
      CloseableIterable<MixedFileScanTask> splitFiles,
      List<MixedFileScanTask> deleteFiles,
      long splitSize,
      int lookback,
      long openFileCost) {
    Function<MixedFileScanTask, Long> weightFunc =
        task -> Math.max(task.file().fileSizeInBytes(), openFileCost);
    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(splitFiles, splitSize, lookback, weightFunc, true),
            splitFiles),
        datafiles -> packingTask(datafiles, deleteFiles));
  }

  private NodeFileScanTask packingTask(
      List<MixedFileScanTask> datafiles, List<MixedFileScanTask> deleteFiles) {
    return new NodeFileScanTask(
        Stream.concat(datafiles.stream(), deleteFiles.stream()).collect(Collectors.toList()));
  }

  public CloseableIterable<CombinedScanTask> combineNode(
      CloseableIterable<NodeFileScanTask> splitFiles,
      long splitSize,
      int lookback,
      long openFileCost) {
    Function<NodeFileScanTask, Long> weightFunc = file -> Math.max(file.cost(), openFileCost);
    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(splitFiles, splitSize, lookback, weightFunc, true),
            splitFiles),
        BaseCombinedScanTask::new);
  }
}
