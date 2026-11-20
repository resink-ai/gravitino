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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.exceptions.ForbiddenException;

/**
 * Binds a catalog to its tenant inside the provider, independent of Gravitino's authorization.
 *
 * <p>The catalog connects to Fluss as the tenant's own user, so whoever may call the catalog acts
 * as that tenant. With Gravitino's authorization bypassed, tenant A could otherwise call {@code
 * fluss_B} and reach B's tables as B. Each catalog therefore records its tenant ({@code
 * resink.tenant}, set by the platform, immutable) and every call checks that the current Gravitino
 * principal is that tenant or one of the platform's service principals ({@code
 * RESINK_SERVICE_PRINCIPALS}, comma separated). A catalog without a tenant answers only to service
 * principals.
 */
final class FlussCallers {

  static final String SERVICE_PRINCIPALS_ENV = "RESINK_SERVICE_PRINCIPALS";

  private FlussCallers() {}

  static Set<String> servicePrincipals() {
    String v = System.getenv(SERVICE_PRINCIPALS_ENV);
    if (v == null) {
      return Set.of();
    }
    return Arrays.stream(v.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }

  /** Throws {@link ForbiddenException} unless the principal may use a catalog of this tenant. */
  static void check(String tenant, String principal, Set<String> services) {
    if (principal != null && services.contains(principal)) {
      return;
    }
    if (tenant != null && !tenant.isEmpty() && tenant.equals(principal)) {
      return;
    }
    throw new ForbiddenException("This catalog can only be used by its own tenant");
  }
}
