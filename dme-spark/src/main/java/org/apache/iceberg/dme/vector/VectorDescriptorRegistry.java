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
package org.apache.iceberg.dme.vector;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Process-local descriptors reconstructed from a DME load response and its physical metadata. */
public final class VectorDescriptorRegistry {
  private static final VectorDescriptorRegistry INSTANCE = new VectorDescriptorRegistry();

  private final Map<String, Map<Integer, VectorDescriptor>> descriptors = new ConcurrentHashMap<>();

  private VectorDescriptorRegistry() {}

  public static VectorDescriptorRegistry get() {
    return INSTANCE;
  }

  public void replace(String tableUuid, Collection<VectorDescriptor> tableDescriptors) {
    Map<Integer, VectorDescriptor> byFieldId = new ConcurrentHashMap<>();
    for (VectorDescriptor descriptor : tableDescriptors) {
      byFieldId.put(descriptor.fieldId(), descriptor);
    }
    descriptors.put(tableUuid, byFieldId);
  }

  public Optional<VectorDescriptor> find(String tableUuid, int fieldId) {
    return Optional.ofNullable(descriptors.getOrDefault(tableUuid, Map.of()).get(fieldId));
  }

  public void remove(String tableUuid) {
    descriptors.remove(tableUuid);
  }
}
