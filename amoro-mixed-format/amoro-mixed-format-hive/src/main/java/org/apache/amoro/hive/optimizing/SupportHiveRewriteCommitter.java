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

import org.apache.amoro.hive.table.SupportHive;
import org.apache.amoro.hive.utils.HivePartitionUtil;
import org.apache.amoro.hive.utils.HiveTableUtil;
import org.apache.amoro.io.SupportsFileSystemOperations;
import org.apache.amoro.optimizing.MixFormatRewriteCommitter;
import org.apache.amoro.optimizing.RewriteResult;
import org.apache.amoro.optimizing.TableFileScanHelper;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.utils.TableFileUtil;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SupportHiveRewriteCommitter extends MixFormatRewriteCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(SupportHiveRewriteCommitter.class);
  private final Set<StructLike> needRewriteHivePartition;
  Map<String, String> partitionPathMap = new HashMap<>();
  PartitionSpec spec;
  Set<String> emptyDir = Sets.newHashSet();

  public SupportHiveRewriteCommitter(
      MixedTable table,
      Set<StructLike> needRewriteHivePartition,
      Set<StructLike> needRewritePartition,
      CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles,
      long snapshotId,
      StructLikeMap<Long> lastPartitionSequence) {
    super(table, needRewritePartition, rewriteFiles, snapshotId, lastPartitionSequence);
    this.spec = table.spec();
    this.needRewriteHivePartition = needRewriteHivePartition;
  }

  public SupportHiveRewriteCommitter(
      MixedTable table,
      Set<StructLike> needRewriteHivePartition,
      Set<StructLike> needRewritePartition,
      CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles,
      String snapshotId,
      StructLikeMap<Long> lastPartitionSequence) {
    super(table, needRewritePartition, rewriteFiles, snapshotId, lastPartitionSequence);
    this.spec = table.spec();
    this.needRewriteHivePartition = needRewriteHivePartition;
  }

  public void beforeCommit() {
    Types.StructType partitionSchema = spec.partitionType();
    for (DataFile addDatafile : addDatafiles) {
      String partition = spec.partitionToPath(addDatafile.partition());
      if (!needRewriteHivePartition.contains(addDatafile.partition())) {
        try {
          moveFileToHive(arcticTable, partition, addDatafile, partitionSchema);
        } catch (Exception e) {
          throw new RuntimeException("move file to hive location failed", e);
        }
      }
    }
  }

  @Override
  public void commitOrClean() {
    beforeCommit();
    super.commitOrClean();
    emptyDir.forEach(
        dir -> {
          try {
            arcticTable.io().deleteFile(dir);
          } catch (Exception e) {
            LOG.warn("delete empty dir {} failed", dir, e);
          }
        });
  }

  @Override
  public RewriteResult result() {
    return RewriteResult.of(
        removeDataFile.size() + removeDeleteFile.size(),
        removeDeleteFile.size(),
        needRewritePartition.size(),
        needRewriteHivePartition.size(),
        needRewritePartition.stream()
            .map(e -> spec.partitionToPath(e))
            .collect(Collectors.joining(",")),
        addDatafiles.size());
  }

  private void moveFileToHive(
      MixedTable table, String partition, DataFile targetFile, Types.StructType partitionSchema)
      throws TException, InterruptedException {
    String partitionPath = partitionPathMap.get(partition);
    if (partitionPath == null) {
      List<String> partitionValues =
          HivePartitionUtil.partitionValuesAsList(targetFile.partition(), partitionSchema);
      if (spec.isUnpartitioned()) {
        Table hiveTable =
            ((SupportHive) table)
                .getHMSClient()
                .run(
                    client -> client.getTable(table.id().getDatabase(), table.id().getTableName()));
        partitionPath = hiveTable.getSd().getLocation();
      } else {
        String hiveSubdirectory =
            table.isKeyedTable()
                ? HiveTableUtil.newHiveSubdirectory(maxTransactionId)
                : HiveTableUtil.newHiveSubdirectory();

        Partition p =
            HivePartitionUtil.getPartition(
                ((SupportHive) table).getHMSClient(), table, partitionValues);
        if (p == null) {
          partitionPath =
              HiveTableUtil.newHiveDataLocation(
                  ((SupportHive) table).hiveLocation(),
                  spec,
                  targetFile.partition(),
                  hiveSubdirectory);
        } else {
          partitionPath = p.getSd().getLocation();
        }
      }
      partitionPathMap.put(partition, partitionPath);
    }
    moveTargetFiles(table, targetFile, partitionPath);
  }

  private void moveTargetFiles(MixedTable arcticTable, DataFile targetFile, String hiveLocation) {
    String oldFilePath = targetFile.path().toString();
    String newFilePath = TableFileUtil.getNewFilePath(hiveLocation, oldFilePath);

    if (!arcticTable.io().exists(newFilePath)) {
      SupportsFileSystemOperations fileIO = null;
      if (arcticTable.io() instanceof SupportsFileSystemOperations) {
        fileIO = (SupportsFileSystemOperations) arcticTable.io();
      } else {
        LOG.error(
            "fileIO does not implement SupportsFileSystemOperations, can't makedir or rename files!!");
      }
      if (!fileIO.exists(hiveLocation)) {
        LOG.debug(
            "{} hive location {} does not exist and need to mkdir before rename",
            arcticTable.id(),
            hiveLocation);
        fileIO.makeDirectories(hiveLocation);
      }
      fileIO.rename(oldFilePath, newFilePath);
      LOG.debug("{} move file from {} to {}", arcticTable.id(), oldFilePath, newFilePath);
    }

    ((StructLike) targetFile).set(1, newFilePath);
    emptyDir.add(TableFileUtil.getFileDir(oldFilePath));
  }
}
