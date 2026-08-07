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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RESTException;

/** Reads the immutable metadata file named by DME's {@code metadata-location} response field. */
public final class DmeLocalMetadataFileReader {
  public TableMetadata read(String metadataLocation) {
    Path path = toLocalPath(metadataLocation);
    try {
      if (!Files.isRegularFile(path)) {
        throw new RESTException("DME metadata file does not exist: %s", metadataLocation);
      }
      if (!Files.isReadable(path)) {
        throw new RESTException("DME metadata file is not readable: %s", metadataLocation);
      }
      return TableMetadataParser.read(org.apache.iceberg.Files.localInput(path.toString()));
    } catch (RESTException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new RESTException(e, "Failed to read DME metadata file: %s", metadataLocation);
    }
  }

  public Path toLocalPath(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw new RESTException("DME load response is missing metadata-location");
    }

    try {
      URI uri = URI.create(metadataLocation);
      Path path;
      if (uri.getScheme() == null) {
        path = Path.of(metadataLocation);
      } else if ("file".equalsIgnoreCase(uri.getScheme())) {
        path = Path.of(uri);
      } else {
        throw new RESTException(
            "DME metadata-location must be an absolute local path or file URI: %s",
            metadataLocation);
      }

      if (!path.isAbsolute()) {
        throw new RESTException(
            "DME metadata-location must be an absolute path: %s", metadataLocation);
      }
      return path.normalize();
    } catch (IllegalArgumentException e) {
      throw new RESTException(e, "Invalid DME metadata-location: %s", metadataLocation);
    }
  }
}
