/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.dme.rest;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.dme.auth.DmeTokenAuthManager;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthProperties;

/** DME REST catalog used exclusively by {@code DmeSparkCatalog}. */
public class DmeRESTCatalog extends RESTCatalog {
  public DmeRESTCatalog() {
    super(DmeRESTCatalog::newClient);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    Map<String, String> resolved = new HashMap<>(properties);
    String configuredAuthType = resolved.get(AuthProperties.AUTH_TYPE);
    Preconditions.checkArgument(
        configuredAuthType == null
            || configuredAuthType.equals(DmeTokenAuthManager.class.getName()),
        "DME Runtime only supports token authentication, not %s",
        configuredAuthType);
    resolved.put(AuthProperties.AUTH_TYPE, DmeTokenAuthManager.class.getName());
    resolved.put(DmeProperties.INTERNAL_CATALOG_NAME, name);
    super.initialize(name, resolved);
  }

  private static DmeRestClient newClient(Map<String, String> properties) {
    HTTPClient client =
        HTTPClient.builder(properties)
            .uri(properties.get(CatalogProperties.URI))
            .withHeaders(RESTUtil.configHeaders(properties))
            .withObjectMapper(DmeRestObjectMapper.create())
            .build();
    return new DmeRestClient(client, properties.get(DmeProperties.INTERNAL_CATALOG_NAME));
  }
}
