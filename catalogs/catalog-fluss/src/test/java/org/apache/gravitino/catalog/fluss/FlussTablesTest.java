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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class FlussTablesTest {

  private static Column col(String name, org.apache.gravitino.rel.types.Type t, boolean nullable) {
    return Column.of(name, t, null, nullable, false, Column.DEFAULT_VALUE_NOT_SET);
  }

  private static final Column ID = col("id", Types.LongType.get(), false);
  private static final Column MSG = col("msg", Types.StringType.get(), true);
  private static final Column DT = col("dt", Types.StringType.get(), false);
  private static final Distribution HASH_ID = Distributions.hash(3, NamedReference.field("id"));

  private static TableInfo info(TableDescriptor d) {
    return TableInfo.of(TablePath.of("tenant_a", "t"), 7L, 1, d, "file:///tmp", 1000L, 2000L);
  }

  // ---- descriptor (create) ------------------------------------------------------------------

  @Test
  void withoutAPrimaryKeyTheTableIsALogTable() {
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID, MSG}, "c", Map.of(), new Transform[0], HASH_ID, null, null);
    assertTrue(d.getSchema().getPrimaryKey().isEmpty());
    assertEquals(3, d.getTableDistribution().get().getBucketCount().get());
    assertEquals(List.of("id"), d.getBucketKeys());
  }

  @Test
  void aPrimaryKeyIndexMakesAPrimaryKeyTable() {
    Index pk = Indexes.primary("pk", new String[][] {{"id"}});
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID, MSG},
            "c",
            Map.of(),
            new Transform[0],
            HASH_ID,
            null,
            new Index[] {pk});
    assertEquals(List.of("id"), d.getSchema().getPrimaryKeyColumnNames());
  }

  @Test
  void aNonPrimaryIndexIsRefused() {
    Index u = Indexes.unique("u", new String[][] {{"id"}});
    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlussTables.descriptor(
                new Column[] {ID}, "", Map.of(), new Transform[0], null, null, new Index[] {u}));
  }

  @Test
  void sortOrdersAreRefused() {
    SortOrder so = SortOrders.ascending(NamedReference.field("id"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlussTables.descriptor(
                new Column[] {ID},
                "",
                Map.of(),
                new Transform[0],
                null,
                new SortOrder[] {so},
                null));
  }

  @Test
  void aPartitionedTableKeepsItsPartitionKeys() {
    Index pk = Indexes.primary("pk", new String[][] {{"id"}, {"dt"}});
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID, MSG, DT},
            "",
            Map.of(),
            new Transform[] {Transforms.identity("dt")},
            Distributions.hash(2, NamedReference.field("id")),
            null,
            new Index[] {pk});
    assertEquals(List.of("dt"), d.getPartitionKeys());
    assertTrue(d.isPartitioned());
    assertEquals(List.of("id"), d.getBucketKeys());
  }

  @Test
  void aPartitionedLogTableIsAccepted() {
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID, DT},
            "",
            Map.of(),
            new Transform[] {Transforms.identity("dt")},
            HASH_ID,
            null,
            null);
    assertEquals(List.of("dt"), d.getPartitionKeys());
    assertTrue(d.getSchema().getPrimaryKey().isEmpty());
  }

  @Test
  void onlyFlussOptionsPassThroughAsTableProperties() {
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID},
            "",
            Map.of(
                "table.log.ttl", "3d",
                "table.datalake.enabled", "true",
                "fluss.buckets", "9",
                "unrelated", "x"),
            new Transform[0],
            HASH_ID,
            null,
            null);
    assertEquals("3d", d.getProperties().get("table.log.ttl"));
    assertEquals("true", d.getProperties().get("table.datalake.enabled"));
    assertFalse(d.getProperties().containsKey("unrelated"));
    assertFalse(d.getProperties().containsKey("fluss.buckets"));
  }

  @Test
  void bucketsPropertyIsUsedWithoutADistribution() {
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID},
            "",
            Map.of("fluss.buckets", "5"),
            new Transform[0],
            null,
            null,
            null);
    assertEquals(5, d.getTableDistribution().get().getBucketCount().get());
  }

  // ---- toTable (properties) -----------------------------------------------------------------

  @Test
  void aLogTableShowsKindBucketsRetentionAndNoLake() {
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID, MSG},
            "c",
            Map.of("table.log.ttl", "3d"),
            new Transform[0],
            HASH_ID,
            null,
            null);
    FlussTable t = FlussTables.toTable(info(d), null, null);
    Map<String, String> p = t.properties();
    assertEquals("log", p.get("fluss.table-type"));
    assertEquals("3", p.get("fluss.buckets"));
    assertEquals("id", p.get("fluss.bucket-keys"));
    assertEquals("3d", p.get("fluss.log-retention"));
    assertEquals("false", p.get("fluss.lake.enabled"));
    assertEquals("7", p.get("fluss.table-id"));
    assertEquals(2, t.columns().length);
    assertEquals(0, t.index().length);
  }

  @Test
  void aPrimaryKeyTableShowsItsKindIndexAndDefaultRetention() {
    Index pk = Indexes.primary("pk", new String[][] {{"id"}});
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID, MSG},
            "",
            Map.of(),
            new Transform[0],
            HASH_ID,
            null,
            new Index[] {pk});
    FlussTable t = FlussTables.toTable(info(d), null, null);
    assertEquals("primary-key", t.properties().get("fluss.table-type"));
    assertEquals("7 d", t.properties().get("fluss.log-retention"));
    assertEquals(1, t.index().length);
    assertFalse(t.columns()[0].nullable());
  }

  @Test
  void aLakeTableShowsFormatLakeNameAndSnapshot() {
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID},
            "",
            Map.of("table.datalake.enabled", "true", "table.datalake.format", "iceberg"),
            new Transform[0],
            HASH_ID,
            null,
            null);
    FlussTable t = FlussTables.toTable(info(d), "shared", 42L);
    Map<String, String> p = t.properties();
    assertEquals("true", p.get("fluss.lake.enabled"));
    assertEquals("iceberg", p.get("fluss.lake.format"));
    assertEquals("42", p.get("fluss.lake.snapshot-id"));
    assertTrue(p.get("fluss.lake.table").endsWith("tenant_a.t"), p.get("fluss.lake.table"));
    assertTrue(p.get("fluss.lake.table").startsWith("shared."));
  }

  @Test
  void aLakeTableWithoutASnapshotYetHasNoSnapshotId() {
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID},
            "",
            Map.of("table.datalake.enabled", "true"),
            new Transform[0],
            HASH_ID,
            null,
            null);
    assertNull(FlussTables.toTable(info(d), null, null).properties().get("fluss.lake.snapshot-id"));
  }

  @Test
  void aPartitionedTableShowsItsPartitioning() {
    Index pk = Indexes.primary("pk", new String[][] {{"id"}, {"dt"}});
    TableDescriptor d =
        FlussTables.descriptor(
            new Column[] {ID, DT},
            "",
            Map.of(),
            new Transform[] {Transforms.identity("dt")},
            Distributions.hash(2, NamedReference.field("id")),
            null,
            new Index[] {pk});
    FlussTable t = FlussTables.toTable(info(d), null, null);
    assertEquals(1, t.partitioning().length);
  }
}
