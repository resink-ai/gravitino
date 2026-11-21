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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.event.AlterCatalogPreEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogPreEvent;
import org.apache.gravitino.listener.api.event.DisableCatalogPreEvent;
import org.apache.gravitino.listener.api.event.DropCatalogPreEvent;
import org.apache.gravitino.listener.api.event.EnableCatalogPreEvent;
import org.apache.gravitino.listener.api.info.CatalogInfo;
import org.junit.jupiter.api.Test;

public class FlussSecretGuardTest {
  private final NameIdentifier id = NameIdentifier.of("lake", "fluss_t");

  private FlussSecretGuard guard(String provider) {
    FlussSecretGuard g = new FlussSecretGuard();
    g.services = Set.of("admin");
    g.providerOf = i -> provider;
    return g;
  }

  private CreateCatalogPreEvent create(String user, String provider, Map<String, String> props) {
    return new CreateCatalogPreEvent(
        user, id, new CatalogInfo("fluss_t", Catalog.Type.RELATIONAL, provider, "", props, null));
  }

  private AlterCatalogPreEvent alter(String user, String key) {
    return new AlterCatalogPreEvent(
        user, id, new CatalogChange[] {CatalogChange.setProperty(key, "x")});
  }

  @Test
  void inlinePasswordIsRefusedOnCreate() {
    FlussSecretGuard g = guard("fluss");
    assertThrows(
        ForbiddenException.class,
        () -> g.onPreEvent(create("admin", "fluss", Map.of("client.security.sasl.password", "x"))));
    assertThrows(
        ForbiddenException.class,
        () ->
            g.onPreEvent(
                create("admin", "fluss", Map.of("client.security.sasl.jaas.config", "x"))));
  }

  @Test
  void referenceIsAllowedOnCreateByTheService() {
    assertDoesNotThrow(
        () ->
            guard("fluss")
                .onPreEvent(
                    create(
                        "admin", "fluss", Map.of("client.security.sasl.password.ref", "fluss_t"))));
  }

  @Test
  void onlyTheServiceMayCreateOrAlterAFlussCatalog() {
    FlussSecretGuard g = guard("fluss");
    assertThrows(
        ForbiddenException.class,
        () -> g.onPreEvent(create("tenant_a", "fluss", Map.of("resink.tenant", "tenant_a"))));
    assertThrows(
        ForbiddenException.class,
        () -> g.onPreEvent(alter("tenant_a", "client.security.sasl.username")));
    assertDoesNotThrow(() -> g.onPreEvent(alter("admin", "lake.catalog")));
  }

  @Test
  void aCatalogWiredToAnotherTenantsUserOrReferenceIsRefusedOnCreateEvenForTheService() {
    FlussSecretGuard g = guard("fluss");
    assertThrows(
        ForbiddenException.class,
        () ->
            g.onPreEvent(
                create(
                    "admin",
                    "fluss",
                    Map.of("resink.tenant", "a", "client.security.sasl.username", "b"))));
    assertThrows(
        ForbiddenException.class,
        () ->
            g.onPreEvent(
                create(
                    "admin",
                    "fluss",
                    Map.of("resink.tenant", "a", "client.security.sasl.password.ref", "fluss_b"))));
    assertDoesNotThrow(
        () ->
            g.onPreEvent(
                create(
                    "admin",
                    "fluss",
                    Map.of(
                        "resink.tenant", "a",
                        "client.security.sasl.username", "a",
                        "client.security.sasl.password.ref", "fluss_a"))));
  }

  @Test
  void onlyTheServiceMayDropDisableOrEnableAFlussCatalog() {
    for (String provider : new String[] {"fluss", null}) { // null: provider cannot be determined
      FlussSecretGuard g = guard(provider);
      assertThrows(
          ForbiddenException.class, () -> g.onPreEvent(new DropCatalogPreEvent("tenant_a", id)));
      assertThrows(
          ForbiddenException.class, () -> g.onPreEvent(new DisableCatalogPreEvent("tenant_a", id)));
      assertThrows(
          ForbiddenException.class, () -> g.onPreEvent(new EnableCatalogPreEvent("tenant_a", id)));
      assertDoesNotThrow(() -> g.onPreEvent(new DropCatalogPreEvent("admin", id)));
      assertDoesNotThrow(() -> g.onPreEvent(new DisableCatalogPreEvent("admin", id)));
    }
    assertDoesNotThrow(
        () -> guard("lakehouse-iceberg").onPreEvent(new DropCatalogPreEvent("tenant_a", id)));
  }

  @Test
  void otherProvidersAreNotTouched() {
    assertDoesNotThrow(
        () ->
            guard("lakehouse-iceberg")
                .onPreEvent(create("tenant_a", "lakehouse-iceberg", Map.of("jdbc-password", "x"))));
    assertDoesNotThrow(
        () -> guard("lakehouse-iceberg").onPreEvent(alter("admin", "jdbc-password")));
  }

  @Test
  void everyVariantOfASecretKeyIsRefusedOnAlterOfAFlussCatalog() {
    FlussSecretGuard g = guard("fluss");
    for (String key :
        new String[] {
          "password",
          "client.security.sasl.password",
          "Client.Security.SASL.PASSWORD",
          "gravitino.bypass.sasl.jaas.config",
          "gravitino.bypass.anything",
          "client.security.sasl.jaas.config",
          "my.Secret.key"
        }) {
      assertThrows(ForbiddenException.class, () -> g.onPreEvent(alter("admin", key)), key);
    }
  }

  @Test
  void anUnknownProviderGetsTheSameKeyPolicy() {
    FlussSecretGuard g = guard(null);
    assertThrows(ForbiddenException.class, () -> g.onPreEvent(alter("admin", "Password")));
    assertDoesNotThrow(() -> g.onPreEvent(alter("admin", "lake.catalog")));
  }

  @Test
  void theKeyPolicyIsCaseInsensitiveAndAllowsOnlyTheReference() {
    assertFalse(SecretKeys.refused("client.security.sasl.password.ref"));
    assertTrue(SecretKeys.refused("CLIENT.SECURITY.SASL.PASSWORD.REF"));
    assertFalse(SecretKeys.refused("lake.catalog"));
  }
}
