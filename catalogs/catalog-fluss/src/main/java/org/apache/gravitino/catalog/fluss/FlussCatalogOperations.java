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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange.ColumnPosition;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fluss databases as schemas and Fluss tables as tables. All calls go to Fluss as the one identity
 * the catalog was configured with; who may call what is decided by Gravitino's own privileges.
 */
public class FlussCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(FlussCatalogOperations.class);
  private static final long TIMEOUT_SECONDS = 30;

  private Connection connection;
  private Admin admin;
  private String lakeCatalog;

  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    Configuration c = new Configuration();
    for (String k :
        new String[] {
          FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS,
          FlussCatalogPropertiesMetadata.SECURITY_PROTOCOL,
          FlussCatalogPropertiesMetadata.SASL_MECHANISM,
          FlussCatalogPropertiesMetadata.SASL_USERNAME,
          FlussCatalogPropertiesMetadata.SASL_PASSWORD
        }) {
      if (conf.get(k) != null) {
        c.setString(k, conf.get(k));
      }
    }
    this.lakeCatalog = conf.get(FlussCatalogPropertiesMetadata.LAKE_CATALOG);
    this.connection = ConnectionFactory.createConnection(c);
    this.admin = connection.getAdmin();
  }

  @Override
  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (Exception e) {
      LOG.warn("closing the Fluss connection failed", e);
    }
  }

  private static <T> T get(CompletableFuture<T> f) throws Exception {
    try {
      return f.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
    }
  }

  private static String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      org.apache.gravitino.Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    try {
      get(admin.listDatabases());
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to list databases on Fluss: %s", e.getMessage());
    }
  }

  // ---- schemas = databases ------------------------------------------------------------------

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      return get(admin.listDatabases()).stream()
          .map(d -> NameIdentifier.of(namespace, d))
          .toArray(NameIdentifier[]::new);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FlussSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      DatabaseDescriptor.Builder b = DatabaseDescriptor.builder().comment(comment);
      if (properties != null) {
        b.customProperties(properties);
      }
      get(admin.createDatabase(ident.name(), b.build(), false));
    } catch (DatabaseAlreadyExistException e) {
      throw new SchemaAlreadyExistsException(e, "Fluss database %s already exists", ident);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return FlussSchema.builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreator(currentUser()).withCreateTime(Instant.now()).build())
        .build();
  }

  @Override
  public FlussSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      DatabaseInfo info = get(admin.getDatabaseInfo(ident.name()));
      return FlussSchema.builder()
          .withName(ident.name())
          .withComment(info.getDatabaseDescriptor().getComment().orElse(null))
          .withProperties(new HashMap<>(info.getDatabaseDescriptor().getCustomProperties()))
          .withAuditInfo(
              AuditInfo.builder()
                  .withCreateTime(Instant.ofEpochMilli(info.getCreatedTime()))
                  .build())
          .build();
    } catch (DatabaseNotExistException e) {
      throw new NoSuchSchemaException(e, "Fluss database %s does not exist", ident);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FlussSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Altering a Fluss database is not supported.");
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      get(admin.dropDatabase(ident.name(), true, cascade));
      return true;
    } catch (DatabaseNotEmptyException e) {
      throw new NonEmptySchemaException(e, "Fluss database %s is not empty", ident);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // ---- tables -------------------------------------------------------------------------------

  private static TablePath path(NameIdentifier ident) {
    String[] levels = ident.namespace().levels();
    return TablePath.of(levels[levels.length - 1], ident.name());
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    String[] levels = namespace.levels();
    try {
      return get(admin.listTables(levels[levels.length - 1])).stream()
          .map(t -> NameIdentifier.of(namespace, t))
          .toArray(NameIdentifier[]::new);
    } catch (DatabaseNotExistException e) {
      throw new NoSuchSchemaException(e, "Fluss database %s does not exist", namespace);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FlussTable loadTable(NameIdentifier ident) throws NoSuchTableException {
    try {
      return toTable(get(admin.getTableInfo(path(ident))));
    } catch (TableNotExistException e) {
      throw new NoSuchTableException(e, "Fluss table %s does not exist", ident);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private FlussTable toTable(TableInfo info) {
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
      try {
        long id = get(admin.getLatestLakeSnapshot(info.getTablePath())).getSnapshotId();
        props.put(LAKE_SNAPSHOT, String.valueOf(id));
      } catch (Exception e) {
        // no tiered snapshot yet
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

  @Override
  public FlussTable createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
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
    try {
      get(admin.createTable(path(ident), tb.build(), false));
    } catch (DatabaseNotExistException e) {
      throw new NoSuchSchemaException(e, "Fluss database does not exist for %s", ident);
    } catch (TableAlreadyExistException e) {
      throw new TableAlreadyExistsException(e, "Fluss table %s already exists", ident);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    LOG.info("Created Fluss table {} as {}", ident, currentUser());
    return loadTable(ident);
  }

  /** The one schema change Fluss allows: add a nullable column at the end. */
  @Override
  public FlussTable alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    List<org.apache.fluss.metadata.TableChange> fc = new ArrayList<>();
    for (TableChange c : changes) {
      if (c instanceof TableChange.AddColumn) {
        TableChange.AddColumn a = (TableChange.AddColumn) c;
        Preconditions.checkArgument(a.fieldName().length == 1, "Nested columns are not supported.");
        Preconditions.checkArgument(
            a.isNullable(),
            "A Fluss column can only be added as nullable (column %s).",
            a.fieldName()[0]);
        Preconditions.checkArgument(
            a.getPosition() == null
                || a.getPosition() instanceof TableChange.Default
                || a.getPosition().toString().toLowerCase().contains("default"),
            "A Fluss column can only be added at the end.");
        fc.add(
            org.apache.fluss.metadata.TableChange.addColumn(
                a.fieldName()[0],
                FlussTypes.toFluss(a.getDataType(), true),
                a.getComment(),
                ColumnPosition.last()));
      } else {
        throw new IllegalArgumentException(
            "Fluss only allows adding a nullable column; refused: " + c);
      }
    }
    try {
      get(admin.alterTable(path(ident), fc, false));
    } catch (TableNotExistException e) {
      throw new NoSuchTableException(e, "Fluss table %s does not exist", ident);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return loadTable(ident);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    try {
      get(admin.dropTable(path(ident), true));
      return true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
