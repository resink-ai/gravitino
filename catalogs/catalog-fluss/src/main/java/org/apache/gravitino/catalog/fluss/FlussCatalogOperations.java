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

import java.time.Instant;
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
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
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
import org.apache.gravitino.exceptions.ForbiddenException;
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
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
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
  private Map<String, String> conf;
  private String lakeCatalog;

  /**
   * Only checks the configuration. The connection (and so the secret) is made on first use, so the
   * catalog can be created before the secret is mounted and a rotated secret is read again after a
   * failed connection.
   */
  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    FlussSecrets.rejectInlineSecrets(conf);
    String ref = conf.get(FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF);
    if (ref != null) {
      FlussSecrets.checkRef(ref);
    }
    this.conf = new HashMap<>(conf);
    this.lakeCatalog = conf.get(FlussCatalogPropertiesMetadata.LAKE_CATALOG);
  }

  private synchronized Admin admin() {
    // every operation goes through here: the caller must be this catalog's tenant (or the platform)
    FlussCallers.check(
        conf.get(FlussCatalogPropertiesMetadata.TENANT),
        PrincipalUtils.getCurrentUserName(),
        FlussCallers.servicePrincipals());
    if (admin == null) {
      Configuration c = new Configuration();
      for (String k :
          new String[] {
            FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS,
            FlussCatalogPropertiesMetadata.SECURITY_PROTOCOL,
            FlussCatalogPropertiesMetadata.SASL_MECHANISM,
            FlussCatalogPropertiesMetadata.SASL_USERNAME
          }) {
        if (conf.get(k) != null) {
          c.setString(k, conf.get(k));
        }
      }
      String ref = conf.get(FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF);
      if (ref != null) {
        c.setString(
            FlussCatalogPropertiesMetadata.FLUSS_SASL_PASSWORD,
            FlussSecrets.resolve(FlussSecrets.secretsDir(), ref));
      }
      connection = ConnectionFactory.createConnection(c);
      admin = connection.getAdmin();
    }
    return admin;
  }

  /** Drops the connection so that the next call reads the secret again. */
  private synchronized void reset() {
    close();
    connection = null;
    admin = null;
  }

  /** Maps a failed Fluss call to the exception the caller should see. */
  private RuntimeException fail(Exception e) {
    if (e instanceof AuthenticationException) {
      reset(); // the secret may have been rotated: read it again next time
      return new ConnectionFailedException("Fluss refused the catalog's credentials");
    }
    if (e instanceof AuthorizationException) {
      return new ForbiddenException("Fluss refused the call: %s", e.getMessage());
    }
    return e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
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
    // Gravitino stores a catalog's properties as given, before initialize runs: refuse a secret
    // value here
    FlussSecrets.rejectInlineSecrets(properties);
    String ref = properties.get(FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF);
    if (ref != null) {
      FlussSecrets.checkRef(ref);
    }
    try {
      get(admin().listDatabases());
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to list databases on Fluss: %s", e.getMessage());
    }
  }

  // ---- schemas = databases ------------------------------------------------------------------

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      return get(admin().listDatabases()).stream()
          .map(d -> NameIdentifier.of(namespace, d))
          .toArray(NameIdentifier[]::new);
    } catch (Exception e) {
      throw fail(e);
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
      get(admin().createDatabase(ident.name(), b.build(), false));
    } catch (DatabaseAlreadyExistException e) {
      throw new SchemaAlreadyExistsException(e, "Fluss database %s already exists", ident);
    } catch (Exception e) {
      throw fail(e);
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
      DatabaseInfo info = get(admin().getDatabaseInfo(ident.name()));
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
      throw fail(e);
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
      get(admin().dropDatabase(ident.name(), true, cascade));
      return true;
    } catch (DatabaseNotEmptyException e) {
      throw new NonEmptySchemaException(e, "Fluss database %s is not empty", ident);
    } catch (Exception e) {
      throw fail(e);
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
      return get(admin().listTables(levels[levels.length - 1])).stream()
          .map(t -> NameIdentifier.of(namespace, t))
          .toArray(NameIdentifier[]::new);
    } catch (DatabaseNotExistException e) {
      throw new NoSuchSchemaException(e, "Fluss database %s does not exist", namespace);
    } catch (Exception e) {
      throw fail(e);
    }
  }

  @Override
  public FlussTable loadTable(NameIdentifier ident) throws NoSuchTableException {
    try {
      TableInfo info = get(admin().getTableInfo(path(ident)));
      Long snapshot = null;
      if ("true".equals(info.getProperties().toMap().get("table.datalake.enabled"))) {
        try {
          snapshot = get(admin().getLatestLakeSnapshot(info.getTablePath())).getSnapshotId();
        } catch (Exception ignored) {
          // no tiered snapshot yet
        }
      }
      return FlussTables.toTable(info, lakeCatalog, snapshot);
    } catch (TableNotExistException e) {
      throw new NoSuchTableException(e, "Fluss table %s does not exist", ident);
    } catch (Exception e) {
      throw fail(e);
    }
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
    TableDescriptor descriptor =
        FlussTables.descriptor(
            columns, comment, properties, partitioning, distribution, sortOrders, indexes);
    try {
      get(admin().createTable(path(ident), descriptor, false));
    } catch (DatabaseNotExistException e) {
      throw new NoSuchSchemaException(e, "Fluss database does not exist for %s", ident);
    } catch (TableAlreadyExistException e) {
      throw new TableAlreadyExistsException(e, "Fluss table %s already exists", ident);
    } catch (Exception e) {
      throw fail(e);
    }
    LOG.info("Created Fluss table {} as {}", ident, currentUser());
    return loadTable(ident);
  }

  /** The one schema change Fluss allows: add a nullable column at the end. */
  @Override
  public FlussTable alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    List<org.apache.fluss.metadata.TableChange> fc = FlussTableChanges.toFluss(changes);
    try {
      get(admin().alterTable(path(ident), fc, false));
    } catch (TableNotExistException e) {
      throw new NoSuchTableException(e, "Fluss table %s does not exist", ident);
    } catch (Exception e) {
      throw fail(e);
    }
    return loadTable(ident);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    try {
      get(admin().dropTable(path(ident), true));
      return true;
    } catch (Exception e) {
      throw fail(e);
    }
  }
}
