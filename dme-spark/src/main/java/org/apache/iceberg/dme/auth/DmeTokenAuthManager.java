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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.dme.rest.DmeProperties;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.util.PropertyUtil;

/** AuthManager SPI implementation that obtains and refreshes a DME X-Token. */
public final class DmeTokenAuthManager implements AuthManager {
  private static final String MACHINE_TOKEN_PATH = "/rest/plat/smapp/v1/sessions";
  private static final String HUMAN_TOKEN_PATH = "/rest/usermgmt/v1/users/sessions";

  private final String catalogName;
  private final HttpClient httpClient = HttpClient.newBuilder().build();
  private final ObjectMapper mapper = new ObjectMapper();
  private volatile Token token;

  public DmeTokenAuthManager(String catalogName) {
    this.catalogName = catalogName;
  }

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    refreshIfNeeded(properties);
    return new DmeTokenAuthSession(this, properties);
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    refreshIfNeeded(properties);
    return new DmeTokenAuthSession(this, properties);
  }

  String token(Map<String, String> properties) {
    refreshIfNeeded(properties);
    return token.value;
  }

  private void refreshIfNeeded(Map<String, String> properties) {
    long now = System.currentTimeMillis();
    long skew =
        PropertyUtil.propertyAsLong(
            properties,
            DmeProperties.AUTH_REFRESH_SKEW_MS,
            DmeProperties.AUTH_REFRESH_SKEW_MS_DEFAULT);
    Token current = token;
    if (current != null && now < current.expiresAtMillis - skew) {
      return;
    }

    synchronized (this) {
      current = token;
      if (current != null && now < current.expiresAtMillis - skew) {
        return;
      }
      String username = properties.get(DmeProperties.AUTH_USERNAME);
      String password = properties.get(DmeProperties.AUTH_PASSWORD);
      Preconditions.checkArgument(
          username != null && !username.isBlank(), "Missing %s", DmeProperties.AUTH_USERNAME);
      Preconditions.checkArgument(
          password != null && !password.isBlank(), "Missing %s", DmeProperties.AUTH_PASSWORD);
      Preconditions.checkArgument(
          properties.containsKey(CatalogProperties.URI), "Missing %s", CatalogProperties.URI);

      token = requestToken(username, password, properties, now);
    }
  }

  private Token requestToken(
      String username, String password, Map<String, String> properties, long nowMillis) {
    String accountType =
        properties.getOrDefault(
            DmeProperties.AUTH_ACCOUNT_TYPE, DmeProperties.AUTH_ACCOUNT_TYPE_MACHINE);
    URI uri = tokenUri(properties.get(CatalogProperties.URI), accountType);
    String requestBody = requestBody(username, password, accountType);
    HttpRequest request =
        HttpRequest.newBuilder(uri)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json;charset=UTF-8")
            .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new RESTException(
            "DME token request failed for catalog %s: HTTP %s", catalogName, response.statusCode());
      }
      return parseToken(response.body(), accountType, nowMillis);
    } catch (IOException e) {
      throw new RESTException(e, "DME token request failed for catalog %s", catalogName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RESTException(e, "DME token request interrupted for catalog %s", catalogName);
    }
  }

  private String requestBody(String username, String password, String accountType) {
    ObjectNode body = mapper.createObjectNode();
    if (DmeProperties.AUTH_ACCOUNT_TYPE_MACHINE.equals(accountType)) {
      body.put("grantType", "password");
      body.put("userName", username);
    } else if (DmeProperties.AUTH_ACCOUNT_TYPE_HUMAN.equals(accountType)) {
      body.put("user_name", username);
    } else {
      throw new IllegalArgumentException(
          String.format("Invalid %s: %s", DmeProperties.AUTH_ACCOUNT_TYPE, accountType));
    }
    body.put("value", password);
    try {
      return mapper.writeValueAsString(body);
    } catch (IOException e) {
      throw new RESTException(e, "Failed to serialize DME token request");
    }
  }

  private Token parseToken(String responseBody, String accountType, long nowMillis) {
    try {
      JsonNode response = mapper.readTree(responseBody);
      if (DmeProperties.AUTH_ACCOUNT_TYPE_MACHINE.equals(accountType)) {
        String value = requiredText(response, "accessSession");
        JsonNode expires = response.get("expires");
        Preconditions.checkArgument(
            expires != null && expires.canConvertToLong() && expires.asLong() > 0,
            "DME machine token response has invalid expires");
        return new Token(value, nowMillis + expires.asLong() * 1_000L);
      }
      return new Token(requiredText(response, "session-id"), Long.MAX_VALUE);
    } catch (IOException e) {
      throw new RESTException(e, "Failed to parse DME token response for catalog %s", catalogName);
    }
  }

  private static String requiredText(JsonNode response, String field) {
    String value = response.path(field).asText(null);
    Preconditions.checkArgument(
        value != null && !value.isBlank(), "DME token response is missing %s", field);
    return value;
  }

  static URI tokenUri(String catalogUri, String accountType) {
    String path =
        DmeProperties.AUTH_ACCOUNT_TYPE_MACHINE.equals(accountType)
            ? MACHINE_TOKEN_PATH
            : DmeProperties.AUTH_ACCOUNT_TYPE_HUMAN.equals(accountType) ? HUMAN_TOKEN_PATH : null;
    Preconditions.checkArgument(
        path != null, "Invalid %s: %s", DmeProperties.AUTH_ACCOUNT_TYPE, accountType);
    try {
      URI base = URI.create(catalogUri);
      return new URI(base.getScheme(), null, base.getHost(), base.getPort(), path, null, null);
    } catch (IllegalArgumentException | URISyntaxException e) {
      throw new RESTException(e, "Invalid DME catalog URI: %s", catalogUri);
    }
  }

  @Override
  public void close() {
    token = null;
  }

  private static final class Token {
    private final String value;
    private final long expiresAtMillis;

    private Token(String value, long expiresAtMillis) {
      this.value = value;
      this.expiresAtMillis = expiresAtMillis;
    }
  }
}
