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
  /** Hidden: never returned by the Gravitino API. */
  public static final String SASL_PASSWORD = "client.security.sasl.password";
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
                  SASL_USERNAME, "SASL user the catalog connects as", false, null, false))
          .put(
              SASL_PASSWORD,
              PropertyEntry.stringOptionalPropertyEntry(
                  SASL_PASSWORD, "SASL password (hidden from the API)", false, null, true))
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
