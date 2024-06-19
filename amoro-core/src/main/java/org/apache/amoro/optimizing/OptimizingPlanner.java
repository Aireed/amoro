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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import java.util.Set;

public abstract class OptimizingPlanner {

  protected Expression filter = Expressions.alwaysTrue();

  public abstract CloseableIterable<?> planFiles();

  public abstract CloseableIterable<TableFileScanHelper.FileScanResult> rewriteFiles();

  public abstract Set<StructLike> baseStoreRewritePartition();

  public abstract Set<StructLike> allRewritePartition();

  public abstract TableSnapshot tableSnapshot();

  public OptimizingPlanner scanFilter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }
}
