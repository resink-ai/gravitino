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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Resolves the catalog's password from a secret reference, so that no password is ever stored in
 * Gravitino's entity store.
 *
 * <p>The catalog holds {@code client.security.sasl.password.ref = <name>}. The password is the
 * content of the file {@code <secrets dir>/<name>}, read each time the provider connects (so a
 * rotated secret is picked up on the next connection). The directory comes from the environment
 * variable {@code RESINK_SECRETS_DIR} (default {@code /run/secrets/resink}); in a cluster it is a
 * mounted Secret volume, locally a volume the platform writes from its encrypted secret store.
 */
final class FlussSecrets {

  static final String SECRETS_DIR_ENV = "RESINK_SECRETS_DIR";
  static final String DEFAULT_SECRETS_DIR = "/run/secrets/resink";

  /**
   * A flat file name: no path separators and no dots, so a reference cannot leave the directory.
   */
  private static final Pattern REF = Pattern.compile("[A-Za-z0-9_-]{1,100}");

  private FlussSecrets() {}

  static Path secretsDir() {
    String d = System.getenv(SECRETS_DIR_ENV);
    return Paths.get(d == null || d.isEmpty() ? DEFAULT_SECRETS_DIR : d);
  }

  static void checkRef(String ref) {
    if (ref == null || !REF.matcher(ref).matches()) {
      throw new IllegalArgumentException(
          FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF
              + " must be a plain name of letters, digits, '_' and '-'");
    }
  }

  /** Rejects any property that would put a secret value in the entity store. */
  static void rejectInlineSecrets(Map<String, String> conf) {
    for (String k : conf.keySet()) {
      String lower = k.toLowerCase();
      boolean secretLike =
          lower.contains("password") || lower.contains("secret") || lower.contains("jaas");
      if (secretLike && !k.equals(FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF)) {
        throw new IllegalArgumentException(
            "Property '"
                + k
                + "' would store a secret in Gravitino; use "
                + FlussCatalogPropertiesMetadata.SASL_PASSWORD_REF
                + " instead");
      }
    }
  }

  /** The password named by the reference. The error never contains the content of a secret. */
  static String resolve(Path dir, String ref) {
    checkRef(ref);
    Path file = dir.resolve(ref);
    try {
      return new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new IllegalStateException(
          "the secret '" + ref + "' cannot be read (is the reference right and the file mounted?)");
    }
  }
}
