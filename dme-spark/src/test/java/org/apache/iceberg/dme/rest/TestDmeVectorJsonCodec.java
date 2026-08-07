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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestDmeVectorJsonCodec {
  @Test
  void emitsDmePrimitiveWithoutElementId() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                22, "vec_col", Types.ListType.ofOptional(23, Types.FloatType.get())));
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName("vectors")
            .withSchema(schema)
            .setProperty(DmeVectorJsonCodec.DECLARATIONS_PROPERTY, "vec_col:1024")
            .build();

    JsonNode root =
        DmeRestObjectMapper.create()
            .readTree(DmeRestObjectMapper.create().writeValueAsString(request));
    JsonNode field = root.path("schema").path("fields").get(0);

    assertThat(field.path("type").asText()).isEqualTo("floatvector");
    assertThat(field.path("dim").asInt()).isEqualTo(1024);
    assertThat(field.has("element-id")).isFalse();
    assertThat(root.path("properties").has(DmeVectorJsonCodec.DECLARATIONS_PROPERTY)).isFalse();
  }
}
