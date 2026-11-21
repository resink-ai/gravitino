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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.fluss.metadata.TableChange.AddColumn;
import org.apache.fluss.metadata.TableChange.ColumnPosition;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class FlussTableChangesTest {

  private static TableChange add(String name, boolean nullable, TableChange.ColumnPosition pos) {
    return TableChange.addColumn(
        new String[] {name}, Types.StringType.get(), "a note", pos, nullable);
  }

  @Test
  void aNullableColumnAtTheEndIsAccepted() {
    List<org.apache.fluss.metadata.TableChange> out =
        FlussTableChanges.toFluss(add("note", true, TableChange.ColumnPosition.defaultPos()));
    assertEquals(1, out.size());
    AddColumn a = (AddColumn) out.get(0);
    assertEquals("note", a.getName());
    assertTrue(a.getDataType().isNullable());
    assertEquals(ColumnPosition.last(), a.getPosition());
  }

  @Test
  void severalAddsAreAccepted() {
    assertEquals(
        2,
        FlussTableChanges.toFluss(
                add("a", true, TableChange.ColumnPosition.defaultPos()),
                add("b", true, TableChange.ColumnPosition.defaultPos()))
            .size());
  }

  @Test
  void aNotNullColumnIsRefused() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTableChanges.toFluss(
                    add("req", false, TableChange.ColumnPosition.defaultPos())));
    assertTrue(e.getMessage().contains("nullable"));
  }

  @Test
  void firstAndAfterPositionsAreRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () -> FlussTableChanges.toFluss(add("a", true, TableChange.ColumnPosition.first())));
    assertThrows(
        IllegalArgumentException.class,
        () -> FlussTableChanges.toFluss(add("a", true, TableChange.ColumnPosition.after("id"))));
  }

  @Test
  void nestedColumnsAreRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlussTableChanges.toFluss(
                TableChange.addColumn(
                    new String[] {"a", "b"},
                    Types.StringType.get(),
                    "",
                    TableChange.ColumnPosition.defaultPos(),
                    true)));
  }

  @Test
  void dropRenameAndOtherChangesAreRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () -> FlussTableChanges.toFluss(TableChange.deleteColumn(new String[] {"msg"}, false)));
    assertThrows(
        IllegalArgumentException.class,
        () -> FlussTableChanges.toFluss(TableChange.renameColumn(new String[] {"msg"}, "m2")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlussTableChanges.toFluss(
                TableChange.updateColumnType(new String[] {"msg"}, Types.LongType.get())));
    assertThrows(
        IllegalArgumentException.class,
        () -> FlussTableChanges.toFluss(TableChange.setProperty("k", "v")));
    assertThrows(
        IllegalArgumentException.class, () -> FlussTableChanges.toFluss(TableChange.rename("t2")));
  }

  @Test
  void aBadChangeAnywhereRefusesTheWholeBatch() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlussTableChanges.toFluss(
                add("ok", true, TableChange.ColumnPosition.defaultPos()),
                TableChange.deleteColumn(new String[] {"msg"}, false)));
  }
}
