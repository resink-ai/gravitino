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
package org.apache.gravitino.catalog.fluss.guard;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.event.AlterCatalogPreEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogPreEvent;
import org.apache.gravitino.listener.api.info.CatalogInfo;
import org.junit.jupiter.api.Test;

public class FlussSecretGuardTest {
  private final FlussSecretGuard guard = new FlussSecretGuard();
  private final NameIdentifier id = NameIdentifier.of("lake", "fluss_t");

  private CreateCatalogPreEvent create(String provider, Map<String, String> props) {
    return new CreateCatalogPreEvent(
        "admin",
        id,
        new CatalogInfo("fluss_t", Catalog.Type.RELATIONAL, provider, "", props, null));
  }

  @Test
  void inlinePasswordIsRefusedOnCreate() {
    assertThrows(
        ForbiddenException.class,
        () -> guard.onPreEvent(create("fluss", Map.of("client.security.sasl.password", "x"))));
    assertThrows(
        ForbiddenException.class,
        () -> guard.onPreEvent(create("fluss", Map.of("client.security.sasl.jaas.config", "x"))));
  }

  @Test
  void referenceIsAllowedOnCreate() {
    assertDoesNotThrow(
        () ->
            guard.onPreEvent(
                create("fluss", Map.of("client.security.sasl.password.ref", "fluss_t"))));
  }

  @Test
  void otherProvidersAreNotTouched() {
    assertDoesNotThrow(
        () -> guard.onPreEvent(create("lakehouse-iceberg", Map.of("jdbc-password", "x"))));
  }

  @Test
  void inlinePasswordIsRefusedOnAlter() {
    CatalogChange[] bad = {CatalogChange.setProperty("client.security.sasl.password", "x")};
    assertThrows(
        ForbiddenException.class,
        () -> guard.onPreEvent(new AlterCatalogPreEvent("admin", id, bad)));
    CatalogChange[] ok = {CatalogChange.setProperty("lake.catalog", "shared")};
    assertDoesNotThrow(() -> guard.onPreEvent(new AlterCatalogPreEvent("admin", id, ok)));
  }
}
