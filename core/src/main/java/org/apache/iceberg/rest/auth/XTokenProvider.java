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
package org.apache.iceberg.rest.auth;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PropertyUtil;

class XTokenProvider implements AutoCloseable {
  private static final String HUMAN_TOKEN_PATH = "/rest/usermgmt/v1/users/sessions";
  private static final String MACHINE_TOKEN_PATH = "/rest/plat/smapp/v1/sessions";

  private final String username;
  private final String password;
  private final UserType userType;
  private final URI tokenUri;
  private final long refreshBeforeMillis;
  private final CloseableHttpClient httpClient;
  private final AtomicReference<Token> cachedToken = new AtomicReference<>();

  static XTokenProvider create(String catalogName, Map<String, String> properties) {
    String username = properties.get(AuthProperties.X_TOKEN_USERNAME);
    String password = properties.get(AuthProperties.X_TOKEN_PASSWORD);
    Preconditions.checkArgument(
        username != null && !username.isEmpty(),
        "Missing required property for catalog %s: %s",
        catalogName,
        AuthProperties.X_TOKEN_USERNAME);
    Preconditions.checkArgument(
        password != null && !password.isEmpty(),
        "Missing required property for catalog %s: %s",
        catalogName,
        AuthProperties.X_TOKEN_PASSWORD);

    UserType userType =
        UserType.fromString(
            PropertyUtil.propertyAsString(
                properties,
                AuthProperties.X_TOKEN_USER_TYPE,
                AuthProperties.X_TOKEN_USER_TYPE_DEFAULT));
    String configuredUri = properties.get(AuthProperties.X_TOKEN_URI);
    String baseUri = properties.get(CatalogProperties.URI);
    Preconditions.checkArgument(
        configuredUri != null || baseUri != null,
        "Missing required property for catalog %s: %s or %s",
        catalogName,
        AuthProperties.X_TOKEN_URI,
        CatalogProperties.URI);
    URI tokenUri =
        URI.create(
            configuredUri != null
                ? configuredUri
                : RESTUtil.stripTrailingSlash(baseUri) + userType.defaultPath());
    long refreshBeforeMillis =
        PropertyUtil.propertyAsLong(
            properties,
            AuthProperties.X_TOKEN_REFRESH_BEFORE_MS,
            AuthProperties.X_TOKEN_REFRESH_BEFORE_MS_DEFAULT);
    Preconditions.checkArgument(
        refreshBeforeMillis >= 0,
        "Invalid value for %s: %s (must be non-negative)",
        AuthProperties.X_TOKEN_REFRESH_BEFORE_MS,
        refreshBeforeMillis);
    return new XTokenProvider(username, password, userType, tokenUri, refreshBeforeMillis);
  }

  XTokenProvider(
      String username, String password, UserType userType, URI tokenUri, long refreshBeforeMillis) {
    this.username = username;
    this.password = password;
    this.userType = userType;
    this.tokenUri = tokenUri;
    this.refreshBeforeMillis = refreshBeforeMillis;
    this.httpClient = HttpClients.custom().build();
  }

  String token() {
    Token current = cachedToken.get();
    if (current != null && !current.expiresSoon(refreshBeforeMillis)) {
      return current.value();
    }

    synchronized (this) {
      current = cachedToken.get();
      if (current == null || current.expiresSoon(refreshBeforeMillis)) {
        current = fetchToken();
        cachedToken.set(current);
      }

      return current.value();
    }
  }

  private Token fetchToken() {
    HttpPut request = new HttpPut(tokenUri);
    request.setEntity(
        new StringEntity(userType.requestBody(username, password), ContentType.APPLICATION_JSON));

    try {
      return httpClient.execute(
          request,
          response -> {
            String responseBody =
                response.getEntity() == null
                    ? ""
                    : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            if (response.getCode() != HttpStatus.SC_OK) {
              throw new RESTException(
                  "Failed to obtain X-Token for user %s: HTTP %s", username, response.getCode());
            }

            return parseToken(responseBody);
          });
    } catch (IOException e) {
      throw new RESTException(e, "Failed to obtain X-Token for user %s", username);
    }
  }

  private Token parseToken(String responseBody) {
    try {
      JsonNode response = JsonUtil.mapper().readTree(responseBody);
      JsonNode tokenNode =
          response.hasNonNull("accessSession")
              ? response.get("accessSession")
              : response.get("session-id");
      Preconditions.checkArgument(
          tokenNode != null && !tokenNode.asText().isEmpty(),
          "Invalid X-Token response: missing accessSession or session-id");
      long expiresMillis = response.path("expires").asLong(0L);
      long expiresAtMillis =
          expiresMillis > 0 ? System.currentTimeMillis() + expiresMillis : Long.MAX_VALUE;
      return new Token(tokenNode.asText(), expiresAtMillis);
    } catch (IOException e) {
      throw new RESTException(e, "Failed to parse X-Token response");
    }
  }

  @Override
  public void close() {
    cachedToken.set(null);
    try {
      httpClient.close();
    } catch (IOException e) {
      throw new RESTException(e, "Failed to close X-Token HTTP client");
    }
  }

  enum UserType {
    HUMAN(HUMAN_TOKEN_PATH),
    MACHINE(MACHINE_TOKEN_PATH);

    private final String defaultPath;

    UserType(String defaultPath) {
      this.defaultPath = defaultPath;
    }

    static UserType fromString(String value) {
      try {
        return valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid value for %s: %s (must be human or machine)",
                AuthProperties.X_TOKEN_USER_TYPE, value),
            e);
      }
    }

    String defaultPath() {
      return defaultPath;
    }

    String requestBody(String username, String password) {
      Map<String, String> fields =
          this == HUMAN
              ? Map.of("user_name", username, "value", password)
              : Map.of("grantType", "password", "userName", username, "value", password);
      try {
        return JsonUtil.mapper().writeValueAsString(fields);
      } catch (IOException e) {
        throw new RESTException(e, "Failed to encode X-Token request");
      }
    }
  }

  private record Token(String value, long expiresAtMillis) {
    boolean expiresSoon(long refreshBeforeMillis) {
      return System.currentTimeMillis() >= expiresAtMillis - refreshBeforeMillis;
    }
  }
}
