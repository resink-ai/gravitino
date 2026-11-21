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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.fluss.metadata.TableChange.ColumnPosition;
import org.apache.gravitino.rel.TableChange;

/** The schema changes Fluss allows: appending nullable columns. Everything else is refused. */
final class FlussTableChanges {

  private FlussTableChanges() {}

  static List<org.apache.fluss.metadata.TableChange> toFluss(TableChange... changes) {
    List<org.apache.fluss.metadata.TableChange> out = new ArrayList<>();
    for (TableChange c : changes) {
      if (!(c instanceof TableChange.AddColumn)) {
        throw new IllegalArgumentException(
            "Fluss only allows adding a nullable column; refused: " + c.getClass().getSimpleName());
      }
      TableChange.AddColumn a = (TableChange.AddColumn) c;
      Preconditions.checkArgument(a.fieldName().length == 1, "Nested columns are not supported.");
      Preconditions.checkArgument(
          a.isNullable(),
          "A Fluss column can only be added as nullable (column %s).",
          a.fieldName()[0]);
      Preconditions.checkArgument(
          a.getPosition() instanceof TableChange.Default,
          "A Fluss column can only be added at the end (column %s).",
          a.fieldName()[0]);
      out.add(
          org.apache.fluss.metadata.TableChange.addColumn(
              a.fieldName()[0],
              FlussTypes.toFluss(a.getDataType(), true),
              a.getComment(),
              ColumnPosition.last()));
    }
    return out;
  }
}
