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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Rewrites Iceberg namespace paths to the DME namespace convention. */
public final class DmePathAdapter {
  private static final String NAMESPACES = "/namespaces/";
  private static final String SEPARATOR = "0x1F";

  private final String catalogName;

  public DmePathAdapter(String catalogName) {
    Preconditions.checkArgument(
        catalogName != null && !catalogName.isBlank(),
        "Missing DME catalog name: %s",
        DmeProperties.INTERNAL_CATALOG_NAME);
    this.catalogName = catalogName;
  }

  public String adapt(String path) {
    if (path == null || path.startsWith("http://") || path.startsWith("https://")) {
      return path;
    }

    int namespaceStart = path.indexOf(NAMESPACES);
    if (namespaceStart < 0) {
      return path;
    }

    int nameStart = namespaceStart + NAMESPACES.length();
    int nameEnd = path.indexOf('/', nameStart);
    if (nameEnd < 0) {
      nameEnd = path.length();
    }

    // Avoid adapting an already adapted path when a client instance is layered more than once.
    String namespace = path.substring(nameStart, nameEnd);
    if (namespace.startsWith(catalogName + SEPARATOR)) {
      return path;
    }

    return path.substring(0, nameStart)
        + catalogName
        + SEPARATOR
        + namespace
        + path.substring(nameEnd);
  }

  String catalogName() {
    return catalogName;
  }
}
