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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private ExecutorService executor;
  private Map<String, String> conf;
  private String lakeCatalog;

  /** How a connection is made and where secrets are read from: replaceable in tests. */
  java.util.function.Function<Configuration, Connection> connector =
      ConnectionFactory::createConnection;

  java.util.function.Supplier<java.nio.file.Path> secretsDir = FlussSecrets::secretsDir;

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
    FlussIdentity.check(conf);
    String ref = conf.get(FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF);
    if (ref != null) {
      FlussSecrets.checkRef(ref);
    }
    this.conf = new HashMap<>(conf);
    this.lakeCatalog = conf.get(FlussCatalogPropertiesMetadata.LAKE_CATALOG);
  }

  /**
   * Every operation starts here, on the request's own thread (the principal is bound to it): the
   * caller must be this catalog's tenant (or the platform).
   */
  private void checkCaller() {
    FlussCallers.check(
        conf.get(FlussCatalogPropertiesMetadata.TENANT),
        PrincipalUtils.getCurrentUserName(),
        FlussCallers.servicePrincipals());
    FlussIdentity.check(
        conf); // the stored properties too: refuse a catalog wired to someone else's credentials
  }

  /**
   * Runs a Fluss call on this catalog's own thread, never on the caller's. The Fluss client's
   * shaded Netty keeps a buffer cache in a thread local of every thread that uses it; on one of
   * Gravitino's pooled web server threads that cache outlives the catalog and, through its classes,
   * keeps the catalog's class loader (and the client's static allocator, tens of MB) for ever. This
   * thread ends with the catalog.
   */
  private <T> T call(java.util.function.Function<Admin, CompletableFuture<T>> f) throws Exception {
    checkCaller();
    try {
      return executor().submit(() -> get(f.apply(admin()))).get();
    } catch (ExecutionException e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
    }
  }

  private synchronized ExecutorService executor() {
    if (executor == null) {
      ClassLoader loader = FlussCatalogOperations.class.getClassLoader();
      executor =
          Executors.newSingleThreadExecutor(
              r -> {
                Thread t = new Thread(r, "fluss-catalog-" + conf.get("resink.tenant"));
                t.setDaemon(true);
                t.setContextClassLoader(loader);
                return t;
              });
    }
    return executor;
  }

  /** Only on the catalog's own thread. */
  private synchronized Admin admin() {
    if (admin == null) {
      Configuration c = new Configuration();
      for (String k :
          new String[] {
            FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS,
            FlussCatalogPropertiesMetadata.SECURITY_PROTOCOL,
            FlussCatalogPropertiesMetadata.SASL_MECHANISM
          }) {
        if (conf.get(k) != null) {
          c.setString(k, conf.get(k));
        }
      }
      String user = FlussIdentity.user(conf);
      if (user != null) {
        c.setString(FlussCatalogPropertiesMetadata.SASL_USERNAME, user);
      }
      String ref = FlussIdentity.ref(conf);
      if (ref != null) {
        c.setString(
            FlussCatalogPropertiesMetadata.FLUSS_SASL_PASSWORD,
            FlussSecrets.resolve(secretsDir.get(), ref));
      }
      try {
        connection = connector.apply(c);
        admin = connection.getAdmin();
      } catch (RuntimeException e) {
        if (connection != null) { // connected, but no admin: do not leave it open
          try {
            connection.close();
          } catch (Exception ignored) {
            // the first failure is the one reported
          }
          connection = null;
        }
        if (causedByAuthentication(e)) { // connecting is where Fluss checks the password
          admin = null; // the next call reads the secret file again
          throw new ConnectionFailedException("Fluss refused the catalog's credentials");
        }
        throw e;
      }
    }
    return admin;
  }

  static boolean causedByAuthentication(Throwable t) {
    for (int i = 0; t != null && i < 10; i++, t = t.getCause()) {
      if (t instanceof AuthenticationException) {
        return true;
      }
    }
    return false;
  }

  /** Drops the connection so that the next call reads the secret again. */
  private void reset() {
    closeConnection(false);
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

  /**
   * Closes the connection on the thread that made it. Never waits while holding the lock: that
   * thread may be waiting for it.
   */
  private void closeConnection(boolean last) {
    Connection c;
    ExecutorService e;
    synchronized (this) {
      c = connection;
      e = executor;
      connection = null;
      admin = null;
      if (last) {
        executor = null;
      }
    }
    try {
      if (c != null) {
        if (e != null) {
          e.submit(
                  () -> {
                    c.close();
                    return null;
                  })
              .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } else {
          c.close();
        }
      }
    } catch (Exception ex) {
      LOG.warn("closing the Fluss connection failed", ex);
    } finally {
      if (last && e != null) {
        e.shutdown(); // its thread, and the thread locals on it, end here
      }
    }
  }

  @Override
  public void close() {
    closeConnection(true);
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
    FlussIdentity.check(properties);
    String ref = properties.get(FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF);
    if (ref != null) {
      FlussSecrets.checkRef(ref);
    }
    try {
      call(a -> a.listDatabases());
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to list databases on Fluss: %s", e.getMessage());
    }
  }

  // ---- schemas = databases ------------------------------------------------------------------

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      return call(a -> a.listDatabases()).stream()
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
      call(a -> a.createDatabase(ident.name(), b.build(), false));
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
      DatabaseInfo info = call(a -> a.getDatabaseInfo(ident.name()));
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
      call(a -> a.dropDatabase(ident.name(), true, cascade));
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
      return call(a -> a.listTables(levels[levels.length - 1])).stream()
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
      TableInfo info = call(a -> a.getTableInfo(path(ident)));
      Long snapshot = null;
      if ("true".equals(info.getProperties().toMap().get("table.datalake.enabled"))) {
        try {
          snapshot = call(a -> a.getLatestLakeSnapshot(info.getTablePath())).getSnapshotId();
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
      call(a -> a.createTable(path(ident), descriptor, false));
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
      call(a -> a.alterTable(path(ident), fc, false));
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
      call(a -> a.dropTable(path(ident), true));
      return true;
    } catch (Exception e) {
      throw fail(e);
    }
  }
}
