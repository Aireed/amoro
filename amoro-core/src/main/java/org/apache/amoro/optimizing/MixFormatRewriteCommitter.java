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
import org.apache.amoro.data.FileNameRules;
import org.apache.amoro.data.PrimaryKeyedFile;
import org.apache.amoro.op.UpdatePartitionProperties;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.table.UnkeyedTable;
import org.apache.amoro.utils.MixedTableUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class MixFormatRewriteCommitter implements RewriteCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(MixFormatRewriteCommitter.class);
  protected Set<StructLike> needRewritePartition;
  protected Set<StructLike> allPartition = Sets.newHashSet();
  protected Set<StructLike> skipPartition = Sets.newHashSet();
  protected CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles;
  long snapshotId;
  protected long maxTransactionId;
  protected Set<DataFile> addDatafiles = Sets.newHashSet();
  protected Set<DataFile> removeDataFile;
  protected Set<DeleteFile> removeDeleteFile;
  protected final MixedTable arcticTable;
  protected final StructLikeMap<Long> lastPartitionSequence;
  private int retryCommit = 0;
  private Set<ContentFile<?>> unCommitFiles = Sets.newHashSet();

  public MixFormatRewriteCommitter(
      MixedTable arcticTable,
      Set<StructLike> needRewritePartition,
      CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles,
      long snapshotId,
      StructLikeMap<Long> lastPartitionSequence) {
    this.arcticTable = arcticTable;
    this.needRewritePartition = needRewritePartition;
    this.rewriteFiles = rewriteFiles;
    this.snapshotId = snapshotId;
    this.lastPartitionSequence = lastPartitionSequence;
  }

  public MixFormatRewriteCommitter(
      MixedTable arcticTable,
      Set<StructLike> needRewritePartition,
      CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles,
      String snapshotId,
      StructLikeMap<Long> lastPartitionSequence) {
    this.arcticTable = arcticTable;
    this.needRewritePartition = needRewritePartition;
    this.rewriteFiles = rewriteFiles;
    this.snapshotId = Long.valueOf(snapshotId);
    this.lastPartitionSequence = lastPartitionSequence;
  }

  @Override
  public RewriteCommitter addFiles(Set<DataFile> addDatafiles) {
    this.addDatafiles.addAll(addDatafiles);
    this.maxTransactionId =
        Math.max(
            this.maxTransactionId,
            addDatafiles.stream()
                .mapToLong(dataFile -> FileNameRules.parseTransactionId(dataFile.path().toString()))
                .max()
                .orElse(0L));
    return this;
  }

  @Override
  public void apply() {
    addDatafiles.forEach(e -> allPartition.add(e.partition()));
  }

  @Override
  public void commitOrClean() {
    try {
      apply();
      commit();
    } catch (Exception e) {
      LOG.info("clean rewrite files");
      cleanFiles(addDatafiles);
      throw new RuntimeException("commit failed, clean rewrite files", e);
    }
  }

  @Override
  public RewriteResult result() {
    return RewriteResult.of(
        removeDataFile.size() + removeDeleteFile.size(),
        removeDeleteFile.size(),
        needRewritePartition.size(),
        0,
        needRewritePartition.stream()
            .map(e -> arcticTable.spec().partitionToPath(e))
            .collect(Collectors.joining(",")),
        addDatafiles.size());
  }

  protected boolean fileInPartitionNeedSkip(StructLike partitionData) {
    if (arcticTable.isUnkeyedTable()) {
      return false;
    }
    StructLikeMap<Long> currentPartitionSequence =
        MixedTableUtil.readOptimizedSequence(arcticTable.asKeyedTable());
    Long optimizedSequence = currentPartitionSequence.getOrDefault(partitionData, -1L);
    Long fromSequence =
        lastPartitionSequence == null
            ? null
            : lastPartitionSequence.getOrDefault(partitionData, -1L);

    return !Objects.equals(optimizedSequence, fromSequence);
  }

  private void confirmFinalCommitFiles() {
    // skip partition if optimized sequence != from sequence
    allPartition.forEach(
        partition -> {
          if (fileInPartitionNeedSkip(partition)) {
            needRewritePartition.remove(partition);
            skipPartition.add(partition);
          }
        });
    rewriteFiles =
        CloseableIterable.filter(
            rewriteFiles,
            fileScanResult -> {
              PrimaryKeyedFile file = (PrimaryKeyedFile) fileScanResult.file();
              return shouldCommit(file, false);
            });
    removeDataFile = Sets.newHashSet();
    removeDeleteFile = Sets.newHashSet();
    rewriteFiles.forEach(
        fileScanResult -> {
          PrimaryKeyedFile file = (PrimaryKeyedFile) fileScanResult.file();
          if (file.type() == DataFileType.BASE_FILE) {
            removeDataFile.add(fileScanResult.file());
          }
          fileScanResult
              .deleteFiles()
              .forEach(
                  deleteFile -> {
                    if (deleteFile.content() == FileContent.POSITION_DELETES) {
                      removeDeleteFile.add((DeleteFile) deleteFile);
                    }
                  });
        });
    addDatafiles =
        addDatafiles.stream()
            .filter(e -> shouldCommit(e, true))
            .collect(ImmutableSet.toImmutableSet());
  }

  private boolean shouldCommit(ContentFile<?> file, boolean newFile) {
    boolean shouldCommit = !skipPartition.contains(file.partition());
    if (newFile && !shouldCommit) {
      unCommitFiles.add(file);
    }
    return shouldCommit;
  }

  private void commit() {
    confirmFinalCommitFiles();
    UnkeyedTable table =
        arcticTable.isKeyedTable()
            ? arcticTable.asKeyedTable().baseTable()
            : arcticTable.asUnkeyedTable();
    Transaction tx = table.newTransaction();
    OverwriteFiles overwriteFiles = tx.newOverwrite().validateFromSnapshot(snapshotId);
    removeDataFile.forEach(
        file -> {
          if (((PrimaryKeyedFile) file).type() == DataFileType.BASE_FILE) {
            overwriteFiles.deleteFile(file);
          }
        });
    addDatafiles.forEach(overwriteFiles::addFile);
    overwriteFiles.commit();
    UpdatePartitionProperties updatePartitionProperties = table.updatePartitionProperties(tx);
    needRewritePartition.forEach(
        partition -> {
          long partitionSequence =
              lastPartitionSequence == null ? 0 : lastPartitionSequence.getOrDefault(partition, 0L);
          updatePartitionProperties.set(
              partition,
              TableProperties.PARTITION_OPTIMIZED_SEQUENCE,
              String.valueOf(Math.max(maxTransactionId, partitionSequence)));
        });
    updatePartitionProperties.commit();
    // Check if the current commit is valid
    if (validCommit()) {
      retryCommit++;
      LOG.info("find commit conflict, retry commit");
      if (retryCommit > 4) {
        throw new RuntimeException("commit failed, after retry commit 4 times");
      }
      commit();
      return;
    }
    tx.commitTransaction();
    try {
      if (!removeDeleteFile.isEmpty()) {
        table
            .newRewrite()
            .validateFromSnapshot(snapshotId)
            .rewriteFiles(ImmutableSet.of(), removeDeleteFile, ImmutableSet.of(), ImmutableSet.of())
            .commit();
      }
    } catch (ValidationException e) {
      LOG.warn("Iceberg RewriteFiles commit failed, but ignore", e);
    }
    cleanFiles(unCommitFiles);
  }

  private boolean validCommit() {
    arcticTable.refresh();
    // Check if the current commit conflicts with the sequence of other commits in the table
    return needRewritePartition.stream().anyMatch(this::fileInPartitionNeedSkip);
  }

  private void cleanFiles(Set<? extends ContentFile<?>> files) {
    files.forEach(
        file -> {
          try {
            arcticTable.io().deleteFile(file.path().toString());
          } catch (Exception ex) {
            LOG.error("delete file {} failed", file.path().toString(), ex);
          }
        });
  }
}
