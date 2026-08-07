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

/** DME-specific catalog properties. */
public final class DmeProperties {
  /** Internal property populated from Spark's catalog name; it is not a user configuration. */
  public static final String INTERNAL_CATALOG_NAME = "dme.internal.catalog-name";

  public static final String AUTH_USERNAME = "rest.auth.username";
  public static final String AUTH_PASSWORD = "rest.auth.password";
  public static final String AUTH_ACCOUNT_TYPE = "rest.auth.account-type";
  public static final String AUTH_ACCOUNT_TYPE_MACHINE = "machine";
  public static final String AUTH_ACCOUNT_TYPE_HUMAN = "human";
  public static final String AUTH_REFRESH_SKEW_MS = "rest.auth.refresh-skew-ms";
  public static final long AUTH_REFRESH_SKEW_MS_DEFAULT = 30_000L;

  private DmeProperties() {}
}
