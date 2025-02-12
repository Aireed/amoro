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

package org.apache.amoro.hive.catalog;

import static org.apache.amoro.table.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class HiveTablePartitionUpperTestHelper extends HiveTableTestHelper {

  public static final String COLUMN_NAME_OP_DAY = "OP_TIME_DAY";

  public static final Schema HIVE_TABLE_SCHEMA =
      TypeUtil.join(
          BasicTableTestHelper.TABLE_SCHEMA,
          new Schema(
              Types.NestedField.required(
                  5, COLUMN_NAME_OP_TIME_WITH_ZONE, Types.TimestampType.withZone()),
              Types.NestedField.required(6, COLUMN_NAME_D, Types.DecimalType.of(10, 0)),
              Types.NestedField.required(7, COLUMN_NAME_OP_DAY, Types.StringType.get())));

  public static final PrimaryKeySpec HIVE_PRIMARY_KEY_SPEC =
      PrimaryKeySpec.builderFor(HIVE_TABLE_SCHEMA).addColumn("id").build();

  public static final PartitionSpec HIVE_SPEC =
      PartitionSpec.builderFor(HIVE_TABLE_SCHEMA).identity(COLUMN_NAME_OP_DAY).build();

  public HiveTablePartitionUpperTestHelper(boolean hasPrimaryKey, boolean hasPartition) {
    super(
        HIVE_TABLE_SCHEMA,
        hasPrimaryKey ? HIVE_PRIMARY_KEY_SPEC : PrimaryKeySpec.noPrimaryKey(),
        hasPartition ? HIVE_SPEC : PartitionSpec.unpartitioned(),
        buildTableFormat(DEFAULT_FILE_FORMAT_DEFAULT));
  }

  public PartitionSpec partitionSpec() {
    return HIVE_SPEC;
  }
}
