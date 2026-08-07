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

/** The logical DME vector contract and the matching physical Iceberg list element ID. */
public final class VectorDescriptor {
  private final int fieldId;
  private final String name;
  private final int dimension;
  private final int elementId;
  private final boolean required;

  public VectorDescriptor(
      int fieldId, String name, int dimension, int elementId, boolean required) {
    this.fieldId = fieldId;
    this.name = name;
    this.dimension = dimension;
    this.elementId = elementId;
    this.required = required;
  }

  public int fieldId() {
    return fieldId;
  }

  public String name() {
    return name;
  }

  public int dimension() {
    return dimension;
  }

  public int elementId() {
    return elementId;
  }

  public boolean required() {
    return required;
  }
}
