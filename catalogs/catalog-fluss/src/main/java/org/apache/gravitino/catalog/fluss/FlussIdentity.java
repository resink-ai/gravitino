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

import java.util.Map;

/**
 * The Fluss user and the secret reference of a tenant's catalog are not settings: they follow from
 * the tenant ({@code resink.tenant}). The platform names the tenant's Fluss user after the tenant
 * and its secret file {@code fluss_<tenant>}. A catalog whose properties say otherwise is refused,
 * so no one can point a catalog at another tenant's credentials by naming them.
 */
final class FlussIdentity {

  static final String REF_PREFIX = "fluss_";

  private FlussIdentity() {}

  static String userFor(String tenant) {
    return tenant;
  }

  static String refFor(String tenant) {
    return REF_PREFIX + tenant;
  }

  /** Refuses a user or reference that is not the one derived from the catalog's tenant. */
  static void check(Map<String, String> conf) {
    String tenant = conf.get(FlussCatalogPropertiesMetadata.TENANT);
    if (tenant == null || tenant.isEmpty()) {
      return; // a catalog without a tenant answers only to the platform (FlussCallers)
    }
    require(conf, FlussCatalogPropertiesMetadata.SASL_USERNAME, userFor(tenant));
    require(conf, FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF, refFor(tenant));
  }

  private static void require(Map<String, String> conf, String key, String derived) {
    String given = conf.get(key);
    if (given != null && !given.equals(derived)) {
      throw new IllegalArgumentException(
          key + " must be the one derived from " + FlussCatalogPropertiesMetadata.TENANT);
    }
  }

  /** The user to connect as: derived from the tenant when there is one. */
  static String user(Map<String, String> conf) {
    String t = conf.get(FlussCatalogPropertiesMetadata.TENANT);
    return t == null || t.isEmpty()
        ? conf.get(FlussCatalogPropertiesMetadata.SASL_USERNAME)
        : userFor(t);
  }

  static String ref(Map<String, String> conf) {
    String t = conf.get(FlussCatalogPropertiesMetadata.TENANT);
    return t == null || t.isEmpty()
        ? conf.get(FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF)
        : refFor(t);
  }
}
