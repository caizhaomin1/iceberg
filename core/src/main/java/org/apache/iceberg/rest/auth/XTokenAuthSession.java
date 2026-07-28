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

import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPHeaders;
import org.apache.iceberg.rest.ImmutableHTTPRequest;

class XTokenAuthSession implements AuthSession {
  static final String TOKEN_HEADER = "X-Token";

  private final XTokenProvider tokenProvider;

  XTokenAuthSession(XTokenProvider tokenProvider) {
    this.tokenProvider = tokenProvider;
  }

  @Override
  public HTTPRequest authenticate(HTTPRequest request) {
    ImmutableHTTPHeaders.Builder builder = ImmutableHTTPHeaders.builder();
    request.headers().entries().stream()
        .filter(header -> !TOKEN_HEADER.equalsIgnoreCase(header.name()))
        .forEach(builder::addEntry);
    builder.addEntry(HTTPHeader.of(TOKEN_HEADER, tokenProvider.token()));
    return ImmutableHTTPRequest.builder().from(request).headers(builder.build()).build();
  }

  @Override
  public void close() {
    tokenProvider.close();
  }
}
