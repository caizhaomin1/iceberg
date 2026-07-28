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
package org.apache.iceberg.rest.auth.token;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenManager {
  private static final Logger LOG = LoggerFactory.getLogger(TokenManager.class);

  private final Map<String, GenerateTokenResp> cache = Maps.newConcurrentMap();
  private final Map<String, Object> locks = Maps.newConcurrentMap();
  private final CloseableHttpClient loginHttpClient = HttpClients.custom().build();
  private static final String MACHINE_TOKEN_URL_PATH = "/rest/plat/smapp/v1/sessions";
  private static final String HUMAN_TOKEN_URL_PATH = "/rest/usermgmt/v1/users/sessions";
  private static final long REFRESH_BEFORE_MILLIS = 60_000;
  private static final int OK_HTTP_CODE = 200;

  private TokenManager() {}

  private static class Holder {
    private static final TokenManager INSTANCE = new TokenManager();
  }

  public static TokenManager getInstance() {
    return Holder.INSTANCE;
  }

  public String get(String username, String password, UserType userType, URI baseUri)
      throws IOException {
    GenerateTokenResp token = cache.get(username);
    if (token != null) {
      if (isExpired(token)) {
        LOG.warn("Token of user {} is expired, we will refresh it now.", username);
      } else {
        return token.getToken();
      }
    }
    return refresh(username, password, userType, baseUri);
  }

  public String refresh(String username, String password, UserType userType, URI baseUri)
      throws IOException {
    synchronized (locks.computeIfAbsent(username, k -> new Object())) {
      GenerateTokenResp token = refreshInternal(username, password, userType, baseUri);
      cache.put(username, token);
      return token.getToken();
    }
  }

  private GenerateTokenResp refreshInternal(
      String username, String password, UserType userType, URI baseUri) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final URI uri =
        URI.create(
            RESTUtil.stripTrailingSlash(
                userType == UserType.HUMAN
                    ? baseUri.toString() + HUMAN_TOKEN_URL_PATH
                    : baseUri.toString() + MACHINE_TOKEN_URL_PATH));
    final HttpUriRequestBase req = new HttpUriRequestBase(HTTPRequest.HTTPMethod.PUT.name(), uri);
    req.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    req.setEntity(
        userType == UserType.HUMAN
            ? new StringEntity(
                mapper.writeValueAsString(new GenerateHumanTokenReq(username, password)))
            : new StringEntity(
                mapper.writeValueAsString(new GenerateMachineTokenReq(username, password))));

    final GenerateTokenResp respBody =
        loginHttpClient.execute(
            req,
            resp -> {
              if (resp.getCode() != OK_HTTP_CODE) {
                throw new RESTException(
                    "Failed to get token of user %s, http_code:%d, detail:%s",
                    username, resp.getCode(), EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8));
              }

              try {
                String bodyStr = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
                return mapper.readValue(bodyStr, GenerateTokenResp.class);
              } catch (ParseException e) {
                throw new HttpException("Failed to convert HTTP response body to string", e);
              }
            });

    respBody.setExpiredAt(
        System.currentTimeMillis() + TimeUnit.MILLISECONDS.toSeconds(respBody.getExpires()));

    LOG.info(
        "Refresh token of user {} succeed, it will expire at {}",
        username,
        Instant.ofEpochMilli(respBody.getExpiredAt()).atZone(ZoneOffset.UTC).toLocalDate());

    return respBody;
  }

  private boolean isExpired(GenerateTokenResp token) {
    return System.currentTimeMillis() > (token.getExpiredAt() - REFRESH_BEFORE_MILLIS);
  }
}
