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
package org.apache.iceberg.dme.auth;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;

/** Dynamic auth session that adds the current DME token to every catalog request. */
final class DmeTokenAuthSession implements AuthSession {
  private final DmeTokenAuthManager manager;
  private final Map<String, String> properties;

  DmeTokenAuthSession(DmeTokenAuthManager manager, Map<String, String> properties) {
    this.manager = manager;
    this.properties = properties;
  }

  @Override
  public HTTPRequest authenticate(HTTPRequest request) {
    Map<String, String> headerValues = new HashMap<>();
    request.headers().entries().forEach(header -> headerValues.put(header.name(), header.value()));
    headerValues.put("X-Token", manager.token(properties));
    return ImmutableHTTPRequest.builder()
        .from(request)
        .headers(HTTPHeaders.of(headerValues))
        .build();
  }

  @Override
  public void close() {
    // The catalog owns the manager and its cached token.
  }
}
