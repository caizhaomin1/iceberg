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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CommitTransactionRequestParser;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.util.JsonUtil;

/** JSON serializers that preserve Iceberg's request contract and replace declared vector fields. */
final class DmeRestRequestSerializers {
  private DmeRestRequestSerializers() {}

  static final class CreateTableSerializer extends JsonSerializer<CreateTableRequest> {
    @Override
    public void serialize(
        CreateTableRequest request, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      ObjectMapper mapper = (ObjectMapper) gen.getCodec();
      ObjectNode root = mapper.createObjectNode();
      root.put("name", request.name());
      if (request.location() != null) {
        root.put("location", request.location());
      }
      root.set(
          "schema",
          parse(mapper, JsonUtil.generate(g -> SchemaParser.toJson(request.schema(), g), false)));
      if (request.spec() != null) {
        root.set(
            "partition-spec",
            parse(
                mapper,
                JsonUtil.generate(g -> PartitionSpecParser.toJson(request.spec(), g), false)));
      }
      if (request.writeOrder() != null) {
        root.set(
            "write-order",
            parse(
                mapper,
                JsonUtil.generate(g -> SortOrderParser.toJson(request.writeOrder(), g), false)));
      }
      ObjectNode properties = root.putObject("properties");
      for (Map.Entry<String, String> entry : request.properties().entrySet()) {
        properties.put(entry.getKey(), entry.getValue());
      }
      root.put("stage-create", request.stageCreate());
      DmeVectorJsonCodec.rewriteCreate(root);
      gen.writeTree(root);
    }
  }

  static final class UpdateTableSerializer extends JsonSerializer<UpdateTableRequest> {
    @Override
    public void serialize(
        UpdateTableRequest request, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      ObjectMapper mapper = (ObjectMapper) gen.getCodec();
      ObjectNode root =
          (ObjectNode)
              parse(
                  mapper,
                  JsonUtil.generate(g -> UpdateTableRequestParser.toJson(request, g), false));
      DmeVectorJsonCodec.rewriteUpdate(root);
      gen.writeTree(root);
    }
  }

  static final class CommitTransactionSerializer extends JsonSerializer<CommitTransactionRequest> {
    @Override
    public void serialize(
        CommitTransactionRequest request, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      ObjectMapper mapper = (ObjectMapper) gen.getCodec();
      ObjectNode root =
          (ObjectNode)
              parse(
                  mapper,
                  JsonUtil.generate(g -> CommitTransactionRequestParser.toJson(request, g), false));
      DmeVectorJsonCodec.rewriteCommit(root);
      gen.writeTree(root);
    }
  }

  private static JsonNode parse(ObjectMapper mapper, String json) throws IOException {
    return mapper.readTree(json);
  }
}
