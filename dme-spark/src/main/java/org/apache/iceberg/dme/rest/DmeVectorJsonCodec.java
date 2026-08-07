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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.dme.vector.DmeVectorDeclarationContext;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

/**
 * Converts only explicitly declared physical list&lt;float&gt; fields to DME's floatvector JSON.
 */
public final class DmeVectorJsonCodec {
  public static final String DECLARATIONS_PROPERTY = "dme.floatvector.columns";

  private DmeVectorJsonCodec() {}

  static void rewriteCreate(ObjectNode request) {
    Map<String, Integer> declarations = declarationsFromProperties(request.with("properties"));
    JsonNode schema = request.get("schema");
    if (schema != null) {
      rewriteSchema(schema, declarations);
    }
  }

  static void rewriteUpdate(ObjectNode request) {
    Map<String, Integer> declarations = DmeVectorDeclarationContext.declarations();
    JsonNode updates = request.get("updates");
    if (updates == null || !updates.isArray()) {
      return;
    }
    for (JsonNode update : updates) {
      if ("add-schema".equals(update.path("action").asText())) {
        rewriteSchema(update.get("schema"), declarations);
      }
    }
  }

  static void rewriteCommit(ObjectNode request) {
    JsonNode changes = request.get("table-changes");
    if (changes != null && changes.isArray()) {
      for (JsonNode change : changes) {
        if (change instanceof ObjectNode) {
          rewriteUpdate((ObjectNode) change);
        }
      }
    }
  }

  private static Map<String, Integer> declarationsFromProperties(ObjectNode properties) {
    JsonNode encoded = properties.remove(DECLARATIONS_PROPERTY);
    Map<String, Integer> declarations = new LinkedHashMap<>();
    if (encoded == null || !encoded.isTextual() || encoded.asText().isBlank()) {
      return declarations;
    }
    for (String declaration : Splitter.on(',').split(encoded.asText())) {
      java.util.List<String> parts = Splitter.on(':').splitToList(declaration);
      if (parts.size() != 2 || parts.get(0).isBlank()) {
        throw new RESTException("Invalid %s value: %s", DECLARATIONS_PROPERTY, encoded.asText());
      }
      try {
        int dimension = Integer.parseInt(parts.get(1));
        if (dimension <= 0) {
          throw new NumberFormatException("dimension must be positive");
        }
        declarations.put(parts.get(0), dimension);
      } catch (NumberFormatException e) {
        throw new RESTException(e, "Invalid FLOATVECTOR declaration: %s", declaration);
      }
    }
    return declarations;
  }

  private static void rewriteSchema(JsonNode schema, Map<String, Integer> declarations) {
    if (schema == null || declarations.isEmpty()) {
      return;
    }
    JsonNode fields = schema.get("fields");
    if (fields == null || !fields.isArray()) {
      return;
    }
    for (JsonNode candidate : fields) {
      if (!(candidate instanceof ObjectNode)) {
        continue;
      }
      ObjectNode field = (ObjectNode) candidate;
      Integer dimension = declarations.get(field.path("name").asText());
      if (dimension == null) {
        continue;
      }
      JsonNode type = field.get("type");
      if (!isOptionalFloatList(type)) {
        throw new RESTException(
            "FLOATVECTOR field %s must be represented as list<optional float>",
            field.path("name").asText());
      }
      field.put("type", "floatvector");
      field.put("dim", dimension);
    }
  }

  private static boolean isOptionalFloatList(JsonNode type) {
    return type != null
        && type.isObject()
        && "list".equals(type.path("type").asText())
        && !type.path("element-required").asBoolean(false)
        && "float".equals(type.path("element").asText());
  }
}
