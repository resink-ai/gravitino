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
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.AlterCatalogPreEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogPreEvent;
import org.apache.gravitino.listener.api.event.PreEvent;

/**
 * Refuses a Fluss catalog that carries a secret value, before Gravitino stores it.
 *
 * <p>Gravitino writes a catalog's entity before the provider validates it, so a refusal inside the
 * provider leaves the refused properties in the entity store. This listener runs on the synchronous
 * pre-events of catalog creation and alteration, which run before anything is stored, and denies
 * (403) the request. It is a plugin (configured with {@code gravitino.eventListener.names}), not a
 * change to Gravitino's core.
 *
 * <p>What is refused: on creation of a catalog with provider {@code fluss}, any property whose name
 * contains "password", "secret" or "jaas" other than the reference {@code
 * client.security.sasl.password.ref}; on alteration of any catalog, setting a property in the
 * {@code client.security.sasl.} namespace that is secret-like (the alter event does not carry the
 * provider).
 */
public class FlussSecretGuard implements EventListenerPlugin {

  static final String PROVIDER = "fluss";
  static final String SASL_PREFIX = "client.security.sasl.";
  static final String REF = "client.security.sasl.password.ref";

  @Override
  public void init(Map<String, String> properties) {}

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public Mode mode() {
    return Mode.SYNC;
  }

  @Override
  public void onPreEvent(PreEvent preEvent) throws ForbiddenException {
    if (preEvent instanceof CreateCatalogPreEvent) {
      var info = ((CreateCatalogPreEvent) preEvent).createCatalogRequest();
      if (PROVIDER.equals(info.provider()) && info.properties() != null) {
        for (String key : info.properties().keySet()) {
          check(key);
        }
      }
    } else if (preEvent instanceof AlterCatalogPreEvent) {
      for (CatalogChange change : ((AlterCatalogPreEvent) preEvent).catalogChanges()) {
        if (change instanceof CatalogChange.SetProperty) {
          String key = ((CatalogChange.SetProperty) change).getProperty();
          if (key.startsWith(SASL_PREFIX)) {
            check(key);
          }
        }
      }
    }
  }

  /** Throws if the property name would carry a secret value. */
  static void check(String key) {
    String lower = key.toLowerCase();
    boolean secretLike =
        lower.contains("password") || lower.contains("secret") || lower.contains("jaas");
    if (secretLike && !REF.equals(key)) {
      throw new ForbiddenException(
          "Property '%s' would store a secret in Gravitino; use %s instead", key, REF);
    }
  }
}
