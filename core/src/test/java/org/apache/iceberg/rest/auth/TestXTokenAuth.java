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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestXTokenAuth {
  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void disabledUsesNativeAuthManager() {
    try (AuthManager manager = AuthManagers.loadAuthManager("catalog", Map.of())) {
      assertThat(manager).isInstanceOf(NoopAuthManager.class);
    }
  }

  @Test
  void enabledLoadsXTokenAuthManager() {
    Map<String, String> properties = Map.of(AuthProperties.X_TOKEN_AUTH_ENABLED, "true");
    try (AuthManager manager = AuthManagers.loadAuthManager("catalog", properties)) {
      assertThat(manager).isInstanceOf(XTokenAuthManager.class);
    }
  }

  @Test
  void enabledRejectsAnotherAuthType() {
    Map<String, String> properties =
        Map.of(
            AuthProperties.X_TOKEN_AUTH_ENABLED,
            "true",
            AuthProperties.AUTH_TYPE,
            AuthProperties.AUTH_TYPE_BASIC);

    assertThatThrownBy(() -> AuthManagers.loadAuthManager("catalog", properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(AuthProperties.AUTH_TYPE);
  }

  @Test
  void humanTokenIsCachedAndAddedToRequest() throws IOException {
    AtomicInteger requests = new AtomicInteger();
    AtomicReference<String> requestBody = new AtomicReference<>();
    startServer(
        XTokenProvider.UserType.HUMAN.defaultPath(),
        exchange -> {
          requests.incrementAndGet();
          requestBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          respond(exchange, "{\"accessSession\":\"human-token\",\"expires\":300000}");
        });

    Map<String, String> properties = properties(XTokenProvider.UserType.HUMAN, 60_000L);
    try (XTokenProvider provider = XTokenProvider.create("catalog", properties);
        AuthSession session = new XTokenAuthSession(provider)) {
      HTTPRequest request =
          ImmutableHTTPRequest.builder()
              .method(HTTPMethod.GET)
              .baseUri(URI.create("http://localhost"))
              .path("v1/config")
              .build();

      HTTPRequest first = session.authenticate(request);
      HTTPRequest second = session.authenticate(request);

      assertThat(first.headers().firstEntry(XTokenAuthSession.TOKEN_HEADER))
          .get()
          .extracting(HTTPHeader::value)
          .isEqualTo("human-token");
      assertThat(second.headers().firstEntry(XTokenAuthSession.TOKEN_HEADER))
          .get()
          .extracting(HTTPHeader::value)
          .isEqualTo("human-token");
      assertThat(requests).hasValue(1);
      assertThat(JsonUtil.mapper().readTree(requestBody.get()))
          .isEqualTo(JsonUtil.mapper().readTree("{\"user_name\":\"alice\",\"value\":\"secret\"}"));
    }
  }

  @Test
  void machineTokenUsesMachineContract() throws IOException {
    AtomicReference<String> requestBody = new AtomicReference<>();
    startServer(
        XTokenProvider.UserType.MACHINE.defaultPath(),
        exchange -> {
          requestBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          respond(exchange, "{\"session-id\":\"machine-token\",\"expires\":300000}");
        });

    try (XTokenProvider provider =
        XTokenProvider.create("catalog", properties(XTokenProvider.UserType.MACHINE, 60_000L))) {
      assertThat(provider.token()).isEqualTo("machine-token");
      assertThat(JsonUtil.mapper().readTree(requestBody.get()))
          .isEqualTo(
              JsonUtil.mapper()
                  .readTree(
                      "{\"grantType\":\"password\",\"userName\":\"alice\","
                          + "\"value\":\"secret\"}"));
    }
  }

  @Test
  void expiredTokenIsRefreshed() throws IOException {
    AtomicInteger requests = new AtomicInteger();
    startServer(
        XTokenProvider.UserType.HUMAN.defaultPath(),
        exchange -> {
          int request = requests.incrementAndGet();
          respond(
              exchange, String.format("{\"accessSession\":\"token-%s\",\"expires\":1}", request));
        });

    try (XTokenProvider provider =
        XTokenProvider.create("catalog", properties(XTokenProvider.UserType.HUMAN, 1L))) {
      assertThat(provider.token()).isEqualTo("token-1");
      assertThat(provider.token()).isEqualTo("token-2");
      assertThat(requests).hasValue(2);
    }
  }

  @Test
  void authenticationReplacesExistingTokenHeader() throws IOException {
    startServer(
        XTokenProvider.UserType.HUMAN.defaultPath(),
        exchange -> respond(exchange, "{\"accessSession\":\"new-token\",\"expires\":300000}"));

    try (XTokenProvider provider =
            XTokenProvider.create("catalog", properties(XTokenProvider.UserType.HUMAN, 60_000L));
        AuthSession session = new XTokenAuthSession(provider)) {
      HTTPRequest request =
          ImmutableHTTPRequest.builder()
              .method(HTTPMethod.GET)
              .baseUri(URI.create("http://localhost"))
              .path("v1/config")
              .headers(HTTPHeaders.of(HTTPHeader.of(XTokenAuthSession.TOKEN_HEADER, "old-token")))
              .build();

      assertThat(session.authenticate(request).headers().firstEntry(XTokenAuthSession.TOKEN_HEADER))
          .get()
          .extracting(HTTPHeader::value)
          .isEqualTo("new-token");
    }
  }

  @Test
  void catalogSendsTokenAndPrefixedNamespacePath() throws IOException {
    AtomicInteger tokenRequests = new AtomicInteger();
    AtomicReference<String> catalogToken = new AtomicReference<>();
    AtomicReference<String> catalogPath = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        XTokenProvider.UserType.HUMAN.defaultPath(),
        exchange -> {
          tokenRequests.incrementAndGet();
          respond(exchange, "{\"accessSession\":\"catalog-token\",\"expires\":300000}");
        });
    server.createContext(
        "/v1/config",
        exchange -> {
          catalogToken.set(exchange.getRequestHeaders().getFirst(XTokenAuthSession.TOKEN_HEADER));
          respond(exchange, "{}");
        });
    server.createContext(
        "/v1/namespaces/catalog0x1Fns/tables",
        exchange -> {
          catalogPath.set(exchange.getRequestURI().getPath());
          catalogToken.set(exchange.getRequestHeaders().getFirst(XTokenAuthSession.TOKEN_HEADER));
          respond(exchange, "{\"identifiers\":[]}");
        });
    server.start();

    Map<String, String> properties =
        Map.of(
            CatalogProperties.URI,
            baseUri(),
            AuthProperties.X_TOKEN_AUTH_ENABLED,
            "true",
            AuthProperties.X_TOKEN_USERNAME,
            "alice",
            AuthProperties.X_TOKEN_PASSWORD,
            "secret",
            RESTCatalogProperties.CATALOG_NAMESPACE_PREFIX_ENABLED,
            "true");

    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize("catalog", properties);
      assertThat(catalog.listTables(Namespace.of("ns"))).isEmpty();
    }

    assertThat(tokenRequests).hasValue(2);
    assertThat(catalogToken).hasValue("catalog-token");
    assertThat(catalogPath).hasValue("/v1/namespaces/catalog0x1Fns/tables");
  }

  private Map<String, String> properties(
      XTokenProvider.UserType userType, long refreshBeforeMillis) {
    return Map.of(
        CatalogProperties.URI,
        baseUri(),
        AuthProperties.X_TOKEN_USERNAME,
        "alice",
        AuthProperties.X_TOKEN_PASSWORD,
        "secret",
        AuthProperties.X_TOKEN_USER_TYPE,
        userType.name(),
        AuthProperties.X_TOKEN_REFRESH_BEFORE_MS,
        Long.toString(refreshBeforeMillis));
  }

  private void startServer(String path, ExchangeHandler handler) throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(path, exchange -> handler.handle(exchange));
    server.start();
  }

  private String baseUri() {
    return String.format("http://localhost:%s", server.getAddress().getPort());
  }

  private static void respond(HttpExchange exchange, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  @FunctionalInterface
  private interface ExchangeHandler {
    void handle(HttpExchange exchange) throws IOException;
  }
}
