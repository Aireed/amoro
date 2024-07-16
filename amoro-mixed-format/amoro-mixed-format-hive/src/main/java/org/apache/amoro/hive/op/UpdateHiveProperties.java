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

package org.apache.amoro.hive.op;

import org.apache.amoro.hive.table.UnkeyedHiveTable;
import org.apache.amoro.hive.utils.HiveTableUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/** @Auth: hzwangtao6 @Time: 2024/6/27 10:35 @Description: */
public class UpdateHiveProperties implements UpdateProperties {
  protected final Map<String, String> propsSet = Maps.newHashMap();
  protected final Set<String> propsDel = Sets.newHashSet();
  protected final UnkeyedHiveTable unKeyedTable;

  public UpdateHiveProperties(UnkeyedHiveTable unKeyedTable) {
    this.unKeyedTable = unKeyedTable;
  }

  @Override
  public UpdateProperties set(String key, String value) {
    if (key.equals(org.apache.amoro.table.TableProperties.OWNER) && StringUtils.isNotBlank(value)) {
      try {
        HiveTableUtil.setTableOwner(unKeyedTable.getHMSClient(), unKeyedTable.id(), value);
      } catch (IOException e) {
        throw new RuntimeException("Set table owner failed " + unKeyedTable.id(), e);
      }
    }
    propsSet.put(key, value);
    return this;
  }

  @Override
  public UpdateProperties remove(String key) {
    propsDel.add(key);
    return this;
  }

  @Override
  public UpdateProperties defaultFormat(FileFormat format) {
    set(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    return this;
  }

  @Override
  public Map<String, String> apply() {
    Map<String, String> newProperties = Maps.newHashMap();
    for (Map.Entry<String, String> entry : unKeyedTable.properties().entrySet()) {
      if (!propsDel.contains(entry.getKey())) {
        newProperties.put(entry.getKey(), entry.getValue());
      }
    }
    newProperties.putAll(propsSet);
    return newProperties;
  }

  @Override
  public void commit() {
    Map<String, String> props = apply();
    commitIcebergTableProperties(unKeyedTable);
  }

  protected void commitIcebergTableProperties(UnkeyedHiveTable unkeyedTable) {
    UpdateProperties updateProperties = unkeyedTable.getIcebergTable().updateProperties();
    propsSet.forEach(updateProperties::set);
    propsDel.forEach(updateProperties::remove);
    updateProperties.commit();
  }
}
