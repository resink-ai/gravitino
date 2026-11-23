/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Fluss client's Netty keeps a buffer cache in a thread local of every thread that uses it. On
 * Gravitino's pooled web server threads that cache outlives a dropped catalog and pins its class
 * loader (about 1,440 classes and the client's static allocator per catalog). So the connection and
 * every call go through a thread that belongs to the catalog and ends with it.
 */
public class FlussOwnThreadTest {

  private static Connection connection(List<Thread> seen) {
    Admin admin =
        (Admin)
            Proxy.newProxyInstance(
                Admin.class.getClassLoader(),
                new Class<?>[] {Admin.class},
                (p, m, a) -> {
                  seen.add(Thread.currentThread());
                  return m.getName().equals("listDatabases")
                      ? CompletableFuture.completedFuture(List.of("t"))
                      : null;
                });
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (p, m, a) -> {
              seen.add(Thread.currentThread());
              return m.getName().equals("getAdmin") ? admin : null;
            });
  }

  @Test
  void everyCallRunsOnTheCatalogsOwnThreadWhichEndsWhenTheCatalogIsClosed(@TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("fluss_t"), "pw");
    List<Thread> seen = new CopyOnWriteArrayList<>();
    FlussCatalogOperations ops = new FlussCatalogOperations();
    ops.initialize(Map.of("bootstrap.servers", "x:1", "resink.tenant", "t"), null, null);
    ops.secretsDir = () -> dir;
    ops.connector = c -> connection(seen);
    // like Gravitino's web server: a pool whose threads outlive the catalog
    ExecutorService webServer = Executors.newFixedThreadPool(2);
    try {
      Namespace ns = Namespace.of("lake", "fluss_t");
      for (int i = 0; i < 4; i++) {
        assertEquals(
            1,
            webServer
                .submit(
                    () ->
                        PrincipalUtils.doAs(
                            new UserPrincipal("t"), () -> ops.listSchemas(ns).length))
                .get());
      }
      assertFalse(seen.isEmpty());
      Thread own = seen.get(0);
      for (Thread t : seen) {
        assertEquals(own, t, "the connection and every call use one thread");
        assertTrue(t.getName().startsWith("fluss-catalog-"), t.getName());
      }
      assertTrue(own.isAlive());
      ops.close();
      own.join(TimeUnit.SECONDS.toMillis(10));
      assertFalse(own.isAlive(), "the catalog's thread, and its thread locals, are gone");
    } finally {
      webServer.shutdownNow();
    }
  }

  @Test
  void aCatalogThatWasNeverUsedHasNoThread() {
    FlussCatalogOperations ops = new FlussCatalogOperations();
    ops.initialize(Map.of("bootstrap.servers", "x:1", "resink.tenant", "t"), null, null);
    ops.close();
    ops.close(); // and closing twice is harmless
  }
}
