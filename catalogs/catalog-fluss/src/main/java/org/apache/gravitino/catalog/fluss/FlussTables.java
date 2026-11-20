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

import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.BUCKETS;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.BUCKET_KEYS;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.FLUSS_OPTION_PREFIX;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.LAKE_ENABLED;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.LAKE_FORMAT;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.LAKE_SNAPSHOT;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.LAKE_TABLE;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.RETENTION;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.SCHEMA_ID;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.TABLE_ID;
import static org.apache.gravitino.catalog.fluss.FlussTablePropertiesMetadata.TABLE_TYPE;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

/** Mapping between Gravitino tables and Fluss table descriptors and infos (no I/O). */
final class FlussTables {

  private FlussTables() {}

  static FlussTable toTable(TableInfo info, String lakeCatalog, Long lakeSnapshotId) {
    List<String> pk = info.getPrimaryKeys();
    FlussColumn[] columns =
        info.getSchema().getColumns().stream()
            .map(
                c ->
                    FlussColumn.builder()
                        .withName(c.getName())
                        .withType(FlussTypes.fromFluss(c.getDataType()))
                        .withComment(c.getComment().orElse(null))
                        .withNullable(c.getDataType().isNullable())
                        .build())
            .toArray(FlussColumn[]::new);

    Map<String, String> props = new HashMap<>();
    // Fluss' own options first, then the derived facts (which win)
    info.getProperties().toMap().forEach(props::put);
    info.getCustomProperties().toMap().forEach(props::put);
    props.put(TABLE_TYPE, info.hasPrimaryKey() ? "primary-key" : "log");
    props.put(BUCKETS, String.valueOf(info.getNumBuckets()));
    props.put(BUCKET_KEYS, String.join(",", info.getBucketKeys()));
    props.put(SCHEMA_ID, String.valueOf(info.getSchemaId()));
    props.put(TABLE_ID, String.valueOf(info.getTableId()));
    String ttl = info.getProperties().toMap().get("table.log.ttl");
    props.put(RETENTION, ttl == null ? "7 d" : ttl);
    boolean lake = "true".equals(info.getProperties().toMap().get("table.datalake.enabled"));
    props.put(LAKE_ENABLED, String.valueOf(lake));
    if (lake) {
      String fmt = info.getProperties().toMap().get("table.datalake.format");
      props.put(LAKE_FORMAT, fmt == null ? "" : fmt);
      TablePath lakePath = info.getLakeTablePath();
      if (lakePath != null) {
        props.put(
            LAKE_TABLE,
            (lakeCatalog == null ? "" : lakeCatalog + ".")
                + lakePath.getDatabaseName()
                + "."
                + lakePath.getTableName());
      }
      if (lakeSnapshotId != null) {
        props.put(LAKE_SNAPSHOT, String.valueOf(lakeSnapshotId));
      }
    }
    return FlussTable.builder()
        .withName(info.getTablePath().getTableName())
        .withComment(info.getComment().orElse(null))
        .withColumns(columns)
        .withProperties(props)
        .withPartitioning(
            info.getPartitionKeys().stream()
                .map(org.apache.gravitino.rel.expressions.transforms.Transforms::identity)
                .toArray(Transform[]::new))
        .withDistribution(
            Distributions.hash(
                info.getNumBuckets(),
                info.getBucketKeys().stream()
                    .map(NamedReference::field)
                    .toArray(NamedReference[]::new)))
        .withIndexes(
            pk.isEmpty()
                ? Indexes.EMPTY_INDEXES
                : new Index[] {
                  Indexes.primary(
                      "PRIMARY_KEY",
                      pk.stream().map(k -> new String[] {k}).toArray(String[][]::new))
                })
        .withAuditInfo(
            AuditInfo.builder()
                .withCreateTime(Instant.ofEpochMilli(info.getCreatedTime()))
                .withLastModifiedTime(Instant.ofEpochMilli(info.getModifiedTime()))
                .build())
        .build();
  }

  static TableDescriptor descriptor(
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes) {
    Preconditions.checkArgument(
        sortOrders == null || sortOrders.length == 0, "Fluss tables have no sort order.");
    Schema.Builder sb = Schema.newBuilder();
    for (Column c : columns) {
      sb.column(c.name(), FlussTypes.toFluss(c.dataType(), c.nullable()));
    }
    if (indexes != null) {
      for (Index i : indexes) {
        if (i.type() == Index.IndexType.PRIMARY_KEY) {
          List<String> pk = new ArrayList<>();
          for (String[] f : i.fieldNames()) {
            pk.add(f[0]);
          }
          sb.primaryKey(pk);
        } else {
          throw new IllegalArgumentException("Fluss tables support only a primary key index.");
        }
      }
    }
    TableDescriptor.Builder tb = TableDescriptor.builder().schema(sb.build()).comment(comment);
    if (properties != null) {
      Map<String, String> flussOptions = new HashMap<>();
      properties.forEach(
          (k, v) -> {
            if (k.startsWith(FLUSS_OPTION_PREFIX)) {
              flussOptions.put(k, v);
            }
          });
      tb.properties(flussOptions);
    }
    if (partitioning != null && partitioning.length > 0) {
      tb.partitionedBy(
          Arrays.stream(partitioning)
              .map(t -> ((NamedReference.FieldReference) t.references()[0]).fieldName()[0])
              .toArray(String[]::new));
    }
    if (distribution != null && distribution.number() > 0) {
      tb.distributedBy(
          distribution.number(),
          Arrays.stream(distribution.expressions())
              .map(e -> ((NamedReference.FieldReference) e).fieldName()[0])
              .toArray(String[]::new));
    } else if (properties != null && properties.get(BUCKETS) != null) {
      tb.distributedBy(Integer.parseInt(properties.get(BUCKETS)));
    }
    return tb.build();
  }
}
