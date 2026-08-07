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

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;

/** A REST client decorator that applies the DME namespace URL convention. */
public final class DmeRestClient implements RESTClient {
  private final RESTClient delegate;
  private final DmePathAdapter pathAdapter;

  public DmeRestClient(RESTClient delegate, String catalogName) {
    this.delegate = Preconditions.checkNotNull(delegate, "Invalid REST client: null");
    this.pathAdapter = new DmePathAdapter(catalogName);
  }

  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    delegate.head(pathAdapter.adapt(path), headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.delete(pathAdapter.adapt(path), responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.delete(
        pathAdapter.adapt(path), queryParams, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.get(pathAdapter.adapt(path), queryParams, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      ParserContext parserContext) {
    return delegate.get(
        pathAdapter.adapt(path), queryParams, responseType, headers, errorHandler, parserContext);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.post(pathAdapter.adapt(path), body, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return delegate.post(
        pathAdapter.adapt(path), body, responseType, headers, errorHandler, responseHeaders);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders,
      ParserContext parserContext) {
    return delegate.post(
        pathAdapter.adapt(path),
        body,
        responseType,
        headers,
        errorHandler,
        responseHeaders,
        parserContext);
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.postForm(
        pathAdapter.adapt(path), formData, responseType, headers, errorHandler);
  }

  @Override
  public RESTClient withAuthSession(AuthSession session) {
    return new DmeRestClient(delegate.withAuthSession(session), pathAdapter.catalogName());
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
