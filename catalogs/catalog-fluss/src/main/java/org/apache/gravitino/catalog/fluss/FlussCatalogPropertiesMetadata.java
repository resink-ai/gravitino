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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class FlussCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String SECURITY_PROTOCOL = "client.security.protocol";
  public static final String SASL_MECHANISM = "client.security.sasl.mechanism";
  public static final String SASL_USERNAME = "client.security.sasl.username";
  /**
   * A reference to the password (a file name in the secrets directory), never the password itself.
   * See {@link FlussSecrets}.
   */
  public static final String SASL_PASSWORD_REF = "client.security.sasl.password.ref";
  /** The Fluss client's own key for the password; set by the provider after resolving the ref. */
  static final String FLUSS_SASL_PASSWORD = "client.security.sasl.password";

  static final String FLUSS_SASL_JAAS = "client.security.sasl.jaas.config";
  /** The tenant this catalog belongs to; only that tenant (and service principals) may call it. */
  public static final String TENANT = "resink.tenant";
  /** The Iceberg catalog that holds the lake copies (shown in the table's lake location). */
  public static final String LAKE_CATALOG = "lake.catalog";

  private static final Map<String, PropertyEntry<?>> ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              BOOTSTRAP_SERVERS,
              PropertyEntry.stringRequiredPropertyEntry(
                  BOOTSTRAP_SERVERS, "Fluss coordinator address(es)", false, false))
          .put(
              SECURITY_PROTOCOL,
              PropertyEntry.stringOptionalPropertyEntry(
                  SECURITY_PROTOCOL, "PLAINTEXT or SASL", false, "PLAINTEXT", false))
          .put(
              SASL_MECHANISM,
              PropertyEntry.stringOptionalPropertyEntry(
                  SASL_MECHANISM, "SASL mechanism", false, "PLAIN", false))
          .put(
              SASL_USERNAME,
              PropertyEntry.stringOptionalPropertyEntry(
                  SASL_USERNAME, "SASL user the catalog connects as", true, null, false))
          .put(
              SASL_PASSWORD_REF,
              PropertyEntry.stringOptionalPropertyEntry(
                  SASL_PASSWORD_REF,
                  "Name of the secret that holds the SASL password",
                  true,
                  null,
                  false))
          // Reserved: Gravitino refuses a catalog created with these (an inline password would be
          // stored in
          // the entity store). The password only ever comes from the secret reference above.
          .put(
              FLUSS_SASL_PASSWORD,
              PropertyEntry.stringReservedPropertyEntry(
                  FLUSS_SASL_PASSWORD, "Not allowed: use " + SASL_PASSWORD_REF, true))
          .put(
              FLUSS_SASL_JAAS,
              PropertyEntry.stringReservedPropertyEntry(
                  FLUSS_SASL_JAAS, "Not allowed: use " + SASL_PASSWORD_REF, true))
          .put(
              TENANT,
              PropertyEntry.stringImmutablePropertyEntry(
                  TENANT, "Tenant that owns this catalog (set once)", false, null, false, false))
          .put(
              LAKE_CATALOG,
              PropertyEntry.stringOptionalPropertyEntry(
                  LAKE_CATALOG,
                  "Name of the Iceberg catalog holding the lake copies",
                  false,
                  null,
                  false))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return ENTRIES;
  }
}
