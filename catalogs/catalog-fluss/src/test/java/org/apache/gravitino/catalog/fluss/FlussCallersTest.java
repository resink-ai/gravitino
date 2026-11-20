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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.Test;

public class FlussCallersTest {
  private static final Set<String> SERVICES = Set.of("admin");

  @Test
  void theTenantAndTheServiceMayCall() {
    assertDoesNotThrow(() -> FlussCallers.check("tenant_b", "tenant_b", SERVICES));
    assertDoesNotThrow(() -> FlussCallers.check("tenant_b", "admin", SERVICES));
  }

  @Test
  void anotherTenantIsRefused() {
    assertThrows(
        ForbiddenException.class, () -> FlussCallers.check("tenant_b", "tenant_a", SERVICES));
  }

  @Test
  void aCatalogWithoutATenantAnswersOnlyToTheService() {
    assertThrows(ForbiddenException.class, () -> FlussCallers.check(null, "tenant_a", SERVICES));
    assertThrows(ForbiddenException.class, () -> FlussCallers.check("", "", SERVICES));
    assertDoesNotThrow(() -> FlussCallers.check(null, "admin", SERVICES));
  }

  @Test
  void noServicePrincipalsConfiguredMeansNoSuchPrincipal() {
    assertThrows(ForbiddenException.class, () -> FlussCallers.check("tenant_b", "admin", Set.of()));
  }
}
