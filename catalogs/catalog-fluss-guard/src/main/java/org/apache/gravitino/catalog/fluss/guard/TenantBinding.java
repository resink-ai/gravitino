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

import java.util.Map;

/**
 * A tenant's Fluss user and secret reference follow from the tenant ({@code resink.tenant}): the
 * user is the tenant, the reference is {@code fluss_<tenant>}. Shared by the guard (refuses a
 * mismatched catalog before it is stored; Gravitino does not call the provider when it creates a
 * catalog) and by the provider (refuses it when it connects).
 */
public final class TenantBinding {

  public static final String TENANT = "resink.tenant";
  public static final String USERNAME = "client.security.sasl.username";
  public static final String REF = SecretKeys.REF;
  public static final String REF_PREFIX = "fluss_";

  private TenantBinding() {}

  public static String userFor(String tenant) {
    return tenant;
  }

  public static String refFor(String tenant) {
    return REF_PREFIX + tenant;
  }

  /**
   * Throws IllegalArgumentException if the user or reference is not the one derived from the
   * tenant.
   */
  public static void check(Map<String, String> conf) {
    String tenant = conf.get(TENANT);
    if (tenant == null || tenant.isEmpty()) {
      return; // a catalog without a tenant answers only to the platform
    }
    require(conf, USERNAME, userFor(tenant));
    require(conf, REF, refFor(tenant));
  }

  private static void require(Map<String, String> conf, String key, String derived) {
    String given = conf.get(key);
    if (given != null && !given.equals(derived)) {
      throw new IllegalArgumentException(key + " must be the one derived from " + TENANT);
    }
  }
}
