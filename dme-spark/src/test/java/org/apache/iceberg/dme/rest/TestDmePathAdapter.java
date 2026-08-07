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

import org.junit.jupiter.api.Test;

class TestDmePathAdapter {
  @Test
  void rewritesNamespaceUsingLiteralSeparator() {
    DmePathAdapter adapter = new DmePathAdapter("catalog_a");

    assertThat(adapter.adapt("v1/prefix/namespaces/ns/tables/t"))
        .isEqualTo("v1/prefix/namespaces/catalog_a0x1Fns/tables/t");
  }

  @Test
  void leavesNonNamespaceAndAbsolutePathsUnchanged() {
    DmePathAdapter adapter = new DmePathAdapter("catalog_a");

    assertThat(adapter.adapt("v1/config")).isEqualTo("v1/config");
    assertThat(adapter.adapt("https://token.example/v1/token"))
        .isEqualTo("https://token.example/v1/token");
  }
}
