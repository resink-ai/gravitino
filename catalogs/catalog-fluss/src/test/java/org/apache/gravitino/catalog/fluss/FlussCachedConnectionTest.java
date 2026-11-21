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

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FlussCachedConnectionTest {

  /** A connection whose admin answers listDatabases with the given outcome. */
  private static Connection connection(boolean authenticationFails) {
    Admin admin =
        (Admin)
            Proxy.newProxyInstance(
                Admin.class.getClassLoader(),
                new Class<?>[] {Admin.class},
                (p, m, a) -> {
                  if (m.getName().equals("listDatabases")) {
                    return authenticationFails
                        ? CompletableFuture.failedFuture(
                            new AuthenticationException("Invalid username or password"))
                        : CompletableFuture.completedFuture(List.of("t"));
                  }
                  return null;
                });
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (p, m, a) -> m.getName().equals("getAdmin") ? admin : null);
  }

  @Test
  void anAuthenticationFailureOnTheCachedConnectionResetsItAndTheNextCallReadsTheNewSecret(
      @TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("fluss_t"), "old-password");
    FlussCatalogOperations ops = new FlussCatalogOperations();
    ops.initialize(Map.of("bootstrap.servers", "x:1", "resink.tenant", "t"), null, null);
    ops.secretsDir = () -> dir;
    List<String> passwordsSeen = new ArrayList<>();
    ops.connector =
        c -> {
          String pw = c.toMap().get("client.security.sasl.password");
          passwordsSeen.add(pw);
          // the first connection is accepted (and cached); once the password was rotated its
          // session is
          // refused; a connection made with the new password works
          return connection(pw.equals("old-password"));
        };
    Namespace ns = Namespace.of("lake", "fluss_t");
    PrincipalUtils.doAs(
        new UserPrincipal("t"),
        () -> {
          // the cached admin is used by an operation that fails authentication: reported as refused
          // credentials
          assertThrows(ConnectionFailedException.class, () -> ops.listSchemas(ns));
          // the secret is replaced; the next call must build a NEW connection and read the new file
          Files.writeString(dir.resolve("fluss_t"), "new-password");
          assertEquals(1, ops.listSchemas(ns).length);
          return null;
        });
    assertEquals(List.of("old-password", "new-password"), passwordsSeen);
  }
}
