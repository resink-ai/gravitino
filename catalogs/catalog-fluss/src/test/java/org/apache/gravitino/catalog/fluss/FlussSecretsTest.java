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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FlussSecretsTest {

  @Test
  void theReferenceNamesAFileAndTheContentIsTrimmed(@TempDir Path dir) throws IOException {
    Files.writeString(dir.resolve("fluss_tenant_a"), "s3cr3t-pw\n");
    assertEquals("s3cr3t-pw", FlussSecrets.resolve(dir, "fluss_tenant_a"));
  }

  @Test
  void aChangedFileIsReadAgain(@TempDir Path dir) throws IOException {
    Files.writeString(dir.resolve("p"), "one");
    assertEquals("one", FlussSecrets.resolve(dir, "p"));
    Files.writeString(dir.resolve("p"), "two");
    assertEquals("two", FlussSecrets.resolve(dir, "p"));
  }

  @Test
  void aReferenceCannotLeaveTheDirectory() {
    for (String bad :
        new String[] {"../etc/passwd", "a/b", "..", ".hidden", "a.b", "", "x y", "/abs", null}) {
      assertThrows(IllegalArgumentException.class, () -> FlussSecrets.checkRef(bad), "ref: " + bad);
    }
  }

  @Test
  void aMissingFileGivesAnErrorWithoutAnyContent(@TempDir Path dir) {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> FlussSecrets.resolve(dir, "nope"));
    assertTrue(e.getMessage().contains("nope"));
    assertFalse(e.getMessage().contains(dir.toString()), "no host path in the message");
  }

  @Test
  void anInlinePasswordIsRefused() {
    for (String k :
        new String[] {
          "client.security.sasl.password",
          "password",
          "gravitino.bypass.sasl.jaas.config",
          "client.security.sasl.secret",
          "Client.Security.SASL.PASSWORD"
        }) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> FlussSecrets.rejectInlineSecrets(Map.of(k, "value-that-must-not-be-echoed")),
              k);
      assertFalse(e.getMessage().contains("value-that-must-not-be-echoed"));
    }
  }

  @Test
  void theReferenceAndOrdinarySettingsAreAccepted() {
    FlussSecrets.rejectInlineSecrets(
        Map.of(
            "bootstrap.servers", "h:9123",
            "client.security.sasl.username", "tenant_a",
            "client.security.sasl.password.ref", "fluss_tenant_a"));
  }

  @Test
  void theCatalogDoesNotDeclareAPasswordProperty() {
    Map<String, PropertyEntry<?>> entries = new FlussCatalogPropertiesMetadata().propertyEntries();
    assertTrue(entries.containsKey("client.security.sasl.password.ref"));
    assertFalse(entries.containsKey("client.security.sasl.password"));
  }

  @Test
  void creatingACatalogWithAnInlinePasswordIsRefusedBeforeAnythingIsStored() {
    // Gravitino stores the properties as given and calls testConnection first: the refusal must
    // happen there
    FlussCatalogOperations ops = new FlussCatalogOperations();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.testConnection(
                    org.apache.gravitino.NameIdentifier.of("lake", "c"),
                    org.apache.gravitino.Catalog.Type.RELATIONAL,
                    "fluss",
                    "",
                    Map.of(
                        "bootstrap.servers", "h:1", "client.security.sasl.password", "inline-pw")));
    assertFalse(e.getMessage().contains("inline-pw"));
  }
}
