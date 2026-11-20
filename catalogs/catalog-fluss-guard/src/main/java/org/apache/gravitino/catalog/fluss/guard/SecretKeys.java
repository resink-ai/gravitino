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

import java.util.Locale;

/**
 * The one rule for which catalog property names are refused because they could carry a secret or
 * reach the client past the platform's settings. Used by the guard (before Gravitino stores a
 * catalog) and by the provider (when it validates), so the two cannot drift apart.
 */
public final class SecretKeys {

  /** The only secret-like property that is allowed: it names a secret, it does not hold one. */
  public static final String REF = "client.security.sasl.password.ref";

  private SecretKeys() {}

  /** True if a Fluss catalog must not be given a property with this name. Case-insensitive. */
  public static boolean refused(String key) {
    if (REF.equals(key)) {
      return false;
    }
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.contains("password")
        || lower.contains("secret")
        || lower.contains("jaas")
        || lower.contains("credential")
        || lower.contains("token")
        || lower.startsWith("gravitino.bypass."); // passed straight to the client, past our mapping
  }
}
