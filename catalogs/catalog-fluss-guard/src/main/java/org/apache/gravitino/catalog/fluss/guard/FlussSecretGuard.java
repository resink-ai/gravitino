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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.AlterCatalogPreEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogPreEvent;
import org.apache.gravitino.listener.api.event.PreEvent;

/**
 * Guards {@code fluss} catalogs before Gravitino stores anything, independent of the authorization
 * plugin.
 *
 * <p>Gravitino writes a catalog's entity before the provider validates it, so a refusal inside the
 * provider is too late. This listener runs on the synchronous pre-events of catalog creation and
 * alteration and denies (403):
 *
 * <ul>
 *   <li>creating or altering a {@code fluss} catalog unless the caller is a service principal
 *       ({@code RESINK_SERVICE_PRINCIPALS}): only the platform wires a catalog to a tenant's
 *       credentials;
 *   <li>any property that {@link SecretKeys#refused} names, on creation, and on alteration of a
 *       {@code fluss} catalog or of a catalog whose provider cannot be determined.
 * </ul>
 */
public class FlussSecretGuard implements EventListenerPlugin {

  static final String PROVIDER = "fluss";
  static final String SERVICE_PRINCIPALS_ENV = "RESINK_SERVICE_PRINCIPALS";

  /**
   * The provider of an existing catalog, null when it cannot be determined. Replaceable in tests.
   */
  Function<NameIdentifier, String> providerOf = FlussSecretGuard::providerFromServer;

  Set<String> services = servicePrincipals(System.getenv(SERVICE_PRINCIPALS_ENV));

  @Override
  public void init(Map<String, String> properties) {}

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public Mode mode() {
    return Mode.SYNC;
  }

  static Set<String> servicePrincipals(String v) {
    if (v == null) {
      return Set.of();
    }
    return Arrays.stream(v.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }

  private static String providerFromServer(NameIdentifier ident) {
    try {
      return GravitinoEnv.getInstance().catalogManager().loadCatalog(ident).provider();
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void onPreEvent(PreEvent preEvent) throws ForbiddenException {
    if (preEvent instanceof CreateCatalogPreEvent) {
      var info = ((CreateCatalogPreEvent) preEvent).createCatalogRequest();
      if (PROVIDER.equals(info.provider())) {
        requireService(preEvent.user());
        if (info.properties() != null) {
          info.properties().keySet().forEach(FlussSecretGuard::check);
          try {
            TenantBinding.check(info.properties());
          } catch (IllegalArgumentException e) {
            throw new ForbiddenException("%s", e.getMessage());
          }
        }
      }
    } else if (preEvent instanceof AlterCatalogPreEvent) {
      String provider = providerOf.apply(preEvent.identifier());
      if (provider == null || PROVIDER.equals(provider)) {
        if (PROVIDER.equals(provider)) {
          requireService(preEvent.user());
        }
        for (CatalogChange change : ((AlterCatalogPreEvent) preEvent).catalogChanges()) {
          if (change instanceof CatalogChange.SetProperty) {
            check(((CatalogChange.SetProperty) change).getProperty());
          }
        }
      }
    }
  }

  private void requireService(String user) {
    if (user == null || !services.contains(user)) {
      throw new ForbiddenException("Only the platform may create or change a fluss catalog");
    }
  }

  /** Throws if the property name would carry a secret value or bypass the platform's settings. */
  static void check(String key) {
    if (SecretKeys.refused(key)) {
      throw new ForbiddenException(
          "Property '%s' is not allowed on a fluss catalog; secrets go in a file named by %s",
          key, SecretKeys.REF);
    }
  }
}
