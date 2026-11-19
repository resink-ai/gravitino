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
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Table properties. Fluss' own options are accepted with their names ("table.log.ttl", ...) as a
 * prefix; the read-only ones below are derived from the cluster.
 */
public class FlussTablePropertiesMetadata extends BasePropertiesMetadata {

  public static final String TABLE_TYPE = "fluss.table-type"; // log | primary-key
  public static final String BUCKETS = "fluss.buckets";
  public static final String BUCKET_KEYS = "fluss.bucket-keys";
  public static final String RETENTION = "fluss.log-retention";
  public static final String LAKE_ENABLED = "fluss.lake.enabled";
  public static final String LAKE_FORMAT = "fluss.lake.format";
  public static final String LAKE_TABLE = "fluss.lake.table";
  public static final String LAKE_SNAPSHOT = "fluss.lake.snapshot-id";
  public static final String SCHEMA_ID = "fluss.schema-id";
  public static final String TABLE_ID = "fluss.table-id";
  /** Passed to Fluss as they are. */
  public static final String FLUSS_OPTION_PREFIX = "table.";

  private static final Map<String, PropertyEntry<?>> ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              BUCKETS,
              PropertyEntry.stringReservedPropertyEntry(BUCKETS, "Number of buckets", false))
          .put(
              FLUSS_OPTION_PREFIX,
              PropertyEntry.stringPropertyPrefixEntry(
                  FLUSS_OPTION_PREFIX, "Fluss table options", false, false, null, false, false))
          .put(
              "fluss.",
              PropertyEntry.stringPropertyPrefixEntry(
                  "fluss.", "Derived, read-only Fluss facts", false, false, null, false, false))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return ENTRIES;
  }
}
