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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class FlussIdentityTest {
  @Test
  void aCatalogWiredToAnotherTenantsUserOrReferenceIsRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlussIdentity.check(
                Map.of("resink.tenant", "a", "client.security.sasl.username", "b")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlussIdentity.check(
                Map.of("resink.tenant", "a", "client.security.sasl.password.ref", "fluss_b")));
  }

  @Test
  void theDerivedValuesAreAccepted() {
    assertDoesNotThrow(
        () ->
            FlussIdentity.check(
                Map.of(
                    "resink.tenant", "a",
                    "client.security.sasl.username", "a",
                    "client.security.sasl.password.ref", "fluss_a")));
    assertDoesNotThrow(() -> FlussIdentity.check(Map.of("resink.tenant", "a")));
  }

  @Test
  void theConnectionUsesTheDerivedUserAndReference() {
    Map<String, String> conf = Map.of("resink.tenant", "a");
    assertEquals("a", FlussIdentity.user(conf));
    assertEquals("fluss_a", FlussIdentity.ref(conf));
  }

  @Test
  void anAuthenticationFailureIsRecognisedThroughWrappingExceptions() {
    org.apache.fluss.exception.AuthenticationException auth =
        new org.apache.fluss.exception.AuthenticationException("Invalid username or password");
    assertEquals(
        true,
        FlussCatalogOperations.causedByAuthentication(
            new RuntimeException("x", new RuntimeException(auth))));
    assertEquals(
        false, FlussCatalogOperations.causedByAuthentication(new RuntimeException("other")));
  }
}
