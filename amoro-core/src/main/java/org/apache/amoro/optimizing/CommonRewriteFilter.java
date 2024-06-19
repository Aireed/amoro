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

import org.apache.amoro.data.DataFileType;
import org.apache.amoro.data.PrimaryKeyedFile;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.TableProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;
import java.util.Set;

public class CommonRewriteFilter implements RewriteFilter {

  private final int rewriteSmallFileThreshold;
  private final Set<StructLike> needRewritePartition = Sets.newHashSet();
  private final Map<String, Integer> partitionSmallFiles = Maps.newHashMap();
  private final PartitionSpec spec;

  public CommonRewriteFilter(
      MixedTable table, CloseableIterable<TableFileScanHelper.FileScanResult> fileScanResults) {
    this.spec = table.spec();
    this.rewriteSmallFileThreshold =
        PropertyUtil.propertyAsInt(
                table.properties(),
                TableProperties.BASE_FILE_INDEX_HASH_BUCKET,
                TableProperties.BASE_FILE_INDEX_HASH_BUCKET_DEFAULT)
            * 2;
    findNeedRewritePartition(table, fileScanResults);
  }

  @Override
  public boolean test(DataFile file) {
    return needRewritePartition.contains(file.partition());
  }

  @Override
  public Set<StructLike> needRewritePartition() {
    return needRewritePartition;
  }

  /**
   * Partitions who will be rewrite partition have change files( insert_file or some delete files)
   * partition whose small files count exceed the threshold.
   *
   * @param table
   * @param fileScanResults:
   */
  private void findNeedRewritePartition(
      MixedTable table, CloseableIterable<TableFileScanHelper.FileScanResult> fileScanResults) {
    long targetSize =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.SELF_OPTIMIZING_TARGET_SIZE,
            TableProperties.SELF_OPTIMIZING_TARGET_SIZE_DEFAULT);

    fileScanResults.forEach(
        fileScanResult -> {
          PrimaryKeyedFile dataFile = (PrimaryKeyedFile) fileScanResult.file();
          if (!fileScanResult.deleteFiles().isEmpty()
              || dataFile.type() == DataFileType.INSERT_FILE) {
            needRewritePartition.add(dataFile.partition());
          } else if (dataFile.fileSizeInBytes()
              < targetSize * BinPackStrategy.MIN_FILE_SIZE_DEFAULT_RATIO) {
            partitionSmallFiles.compute(
                spec.partitionToPath(dataFile.partition()),
                (k, v) -> {
                  if (v == null) {
                    v = 0;
                  }
                  return v + 1;
                });
            if (partitionSmallFiles.get(spec.partitionToPath(dataFile.partition()))
                > rewriteSmallFileThreshold) {
              needRewritePartition.add(dataFile.partition());
            }
          }
        });
  }
}
