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

public class RewriteResult {

  private final int rewriteFileCount;
  private final int rewriteDeleteFileCount;
  private final int rewritePartitionCount;
  private final int rewriteBaseStorePartitionCount;
  private final String rewritePartitions;
  private final int addFileCount;

  private RewriteResult(
      int rewriteFileCount,
      int rewriteDeleteFileCount,
      int rewritePartitionCount,
      int rewriteBaseStorePartitionCount,
      String rewritePartitions,
      int addFileCount) {
    this.rewriteFileCount = rewriteFileCount;
    this.rewriteDeleteFileCount = rewriteDeleteFileCount;
    this.rewritePartitionCount = rewritePartitionCount;
    this.rewriteBaseStorePartitionCount = rewriteBaseStorePartitionCount;
    this.rewritePartitions = rewritePartitions;
    this.addFileCount = addFileCount;
  }

  public static RewriteResult of(
      int rewriteFileCount,
      int rewriteDeleteFileCount,
      int rewritePartitionCount,
      int rewriteBaseStorePartitionCount,
      String rewritePartitions,
      int addFileCount) {
    return new RewriteResult(
        rewriteFileCount,
        rewriteDeleteFileCount,
        rewritePartitionCount,
        rewriteBaseStorePartitionCount,
        rewritePartitions,
        addFileCount);
  }

  public static RewriteResult empty() {
    return new RewriteResult(0, 0, 0, 0, "", 0);
  }

  public int rewriteFileCount() {
    return rewriteFileCount;
  }

  public int rewriteDeleteFileCount() {
    return rewriteDeleteFileCount;
  }

  public int rewritePartitionCount() {
    return rewritePartitionCount;
  }

  public int rewriteBaseStorePartitionCount() {
    return rewriteBaseStorePartitionCount;
  }

  public String rewritePartitions() {
    return rewritePartitions;
  }

  public int addFileCount() {
    return addFileCount;
  }
}
