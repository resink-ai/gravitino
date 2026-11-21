/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.catalog.fluss;

import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;

public class FlussTable extends BaseTable {
  private FlussTable() {}

  @Override
  protected TableOperations newOps() {
    throw new UnsupportedOperationException("Fluss tables have no table operations in Gravitino.");
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends BaseTableBuilder<Builder, FlussTable> {
    private Builder() {}

    @Override
    protected FlussTable internalBuild() {
      FlussTable t = new FlussTable();
      t.name = name;
      t.comment = comment;
      t.properties = properties;
      t.columns = columns;
      t.partitioning = partitioning;
      t.sortOrders = sortOrders;
      t.distribution = distribution;
      t.indexes = indexes;
      t.auditInfo = auditInfo;
      return t;
    }
  }
}
