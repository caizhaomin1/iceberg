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

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.dme.rest.DmeProperties;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;
import org.junit.jupiter.api.Test;

class TestDmeTokenAuthManager {
  @Test
  void buildsFixedTokenEndpointsFromCatalogUri() {
    String catalogUri = "https://dme.example:8443/catalog/v1";

    assertThat(DmeTokenAuthManager.tokenUri(catalogUri, DmeProperties.AUTH_ACCOUNT_TYPE_MACHINE))
        .hasToString("https://dme.example:8443/rest/plat/smapp/v1/sessions");
    assertThat(DmeTokenAuthManager.tokenUri(catalogUri, DmeProperties.AUTH_ACCOUNT_TYPE_HUMAN))
        .hasToString("https://dme.example:8443/rest/usermgmt/v1/users/sessions");
  }

  @Test
  void usesMachineAccountPutContractAndAddsAccessSession() throws IOException {
    AtomicReference<RecordedRequest> recorded = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/rest/plat/smapp/v1/sessions",
        exchange -> {
          recorded.set(record(exchange));
          respond(exchange, "{\"accessSession\":\"machine-token\",\"expires\":1800}");
        });
    server.start();

    try {
      int port = server.getAddress().getPort();
      DmeTokenAuthManager manager = new DmeTokenAuthManager("dme");
      AuthSession session = manager.catalogSession(null, properties(port, "machine"));

      assertThat(
              session
                  .authenticate(request())
                  .headers()
                  .entries("X-Token")
                  .iterator()
                  .next()
                  .value())
          .isEqualTo("machine-token");
      assertThat(recorded.get().method).isEqualTo("PUT");
      assertThat(recorded.get().contentType).isEqualTo("application/json;charset=UTF-8");
      assertThat(recorded.get().body)
          .isEqualTo("{\"grantType\":\"password\",\"userName\":\"user\",\"value\":\"password\"}");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void usesHumanAccountPutContractAndAddsSessionId() throws IOException {
    AtomicReference<RecordedRequest> recorded = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/rest/usermgmt/v1/users/sessions",
        exchange -> {
          recorded.set(record(exchange));
          respond(exchange, "{\"session-id\":\"human-token\"}");
        });
    server.start();

    try {
      DmeTokenAuthManager manager = new DmeTokenAuthManager("dme");
      AuthSession session =
          manager.catalogSession(null, properties(server.getAddress().getPort(), "human"));

      assertThat(
              session
                  .authenticate(request())
                  .headers()
                  .entries("X-Token")
                  .iterator()
                  .next()
                  .value())
          .isEqualTo("human-token");
      assertThat(recorded.get().method).isEqualTo("PUT");
      assertThat(recorded.get().body).isEqualTo("{\"user_name\":\"user\",\"value\":\"password\"}");
    } finally {
      server.stop(0);
    }
  }

  private static Map<String, String> properties(int port, String accountType) {
    return Map.of(
        CatalogProperties.URI,
        "http://localhost:" + port,
        DmeProperties.AUTH_USERNAME,
        "user",
        DmeProperties.AUTH_PASSWORD,
        "password",
        DmeProperties.AUTH_ACCOUNT_TYPE,
        accountType);
  }

  private static HTTPRequest request() {
    return ImmutableHTTPRequest.builder()
        .baseUri(URI.create("http://localhost"))
        .method(HTTPRequest.HTTPMethod.GET)
        .path("v1/config")
        .queryParameters(Map.of())
        .build();
  }

  private static RecordedRequest record(HttpExchange exchange) throws IOException {
    return new RecordedRequest(
        exchange.getRequestMethod(),
        exchange.getRequestHeaders().getFirst("Content-Type"),
        new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
  }

  private static void respond(HttpExchange exchange, String body) throws IOException {
    byte[] response = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, response.length);
    exchange.getResponseBody().write(response);
    exchange.close();
  }

  private static final class RecordedRequest {
    private final String method;
    private final String contentType;
    private final String body;

    private RecordedRequest(String method, String contentType, String body) {
      this.method = method;
      this.contentType = contentType;
      this.body = body;
    }
  }
}
