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
package org.apache.iceberg.dme.spark;

import java.io.IOException;
import java.io.Serializable;
import org.apache.iceberg.dme.vector.DmeVectorValidator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.sources.Filter;

/** Preserves Spark's overwrite capabilities while validating every produced InternalRow. */
final class DmeValidatingWriteBuilder
    implements WriteBuilder, SupportsDynamicOverwrite, SupportsOverwrite {
  private final WriteBuilder delegate;
  private final DmeVectorValidator validator;

  DmeValidatingWriteBuilder(WriteBuilder delegate, DmeVectorValidator validator) {
    this.delegate = delegate;
    this.validator = validator;
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    ((SupportsDynamicOverwrite) delegate).overwriteDynamicPartitions();
    return this;
  }

  @Override
  public WriteBuilder overwrite(Predicate[] filters) {
    ((SupportsOverwrite) delegate).overwrite(filters);
    return this;
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    ((SupportsOverwrite) delegate).overwrite(filters);
    return this;
  }

  @Override
  public Write build() {
    return new ValidatingWrite(delegate.build(), validator);
  }

  /**
   * Spark 3.5 obtains a {@link Write} from the builder and then selects {@code toBatch()} or {@code
   * toStreaming()}. Delegating at this level preserves Iceberg's write requirements.
   */
  private static final class ValidatingWrite implements Write {
    private final Write delegate;
    private final DmeVectorValidator validator;

    private ValidatingWrite(Write delegate, DmeVectorValidator validator) {
      this.delegate = delegate;
      this.validator = validator;
    }

    @Override
    public String description() {
      return delegate.description();
    }

    @Override
    public BatchWrite toBatch() {
      return new ValidatingBatchWrite(delegate.toBatch(), validator);
    }

    @Override
    public StreamingWrite toStreaming() {
      return new ValidatingStreamingWrite(delegate.toStreaming(), validator);
    }

    @Override
    public org.apache.spark.sql.connector.metric.CustomMetric[] supportedCustomMetrics() {
      return delegate.supportedCustomMetrics();
    }
  }

  private static final class ValidatingBatchWrite implements BatchWrite, Serializable {
    private final BatchWrite delegate;
    private final DmeVectorValidator validator;

    private ValidatingBatchWrite(BatchWrite delegate, DmeVectorValidator validator) {
      this.delegate = delegate;
      this.validator = validator;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      DataWriterFactory factory = delegate.createBatchWriterFactory(info);
      return new ValidatingDataWriterFactory(factory, validator);
    }

    @Override
    public boolean useCommitCoordinator() {
      return delegate.useCommitCoordinator();
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
      delegate.onDataWriterCommit(message);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      delegate.commit(messages);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      delegate.abort(messages);
    }
  }

  private static final class ValidatingStreamingWrite implements StreamingWrite, Serializable {
    private final StreamingWrite delegate;
    private final DmeVectorValidator validator;

    private ValidatingStreamingWrite(StreamingWrite delegate, DmeVectorValidator validator) {
      this.delegate = delegate;
      this.validator = validator;
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
      StreamingDataWriterFactory factory = delegate.createStreamingWriterFactory(info);
      return new ValidatingStreamingDataWriterFactory(factory, validator);
    }

    @Override
    public boolean useCommitCoordinator() {
      return delegate.useCommitCoordinator();
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
      delegate.commit(epochId, messages);
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
      delegate.abort(epochId, messages);
    }
  }

  /** The writer factory is what Spark serializes to executors; do not capture BatchWrite. */
  private static final class ValidatingDataWriterFactory implements DataWriterFactory {
    private final DataWriterFactory delegate;
    private final DmeVectorValidator validator;

    private ValidatingDataWriterFactory(DataWriterFactory delegate, DmeVectorValidator validator) {
      this.delegate = delegate;
      this.validator = validator;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new ValidatingDataWriter(delegate.createWriter(partitionId, taskId), validator);
    }
  }

  private static final class ValidatingStreamingDataWriterFactory
      implements StreamingDataWriterFactory {
    private final StreamingDataWriterFactory delegate;
    private final DmeVectorValidator validator;

    private ValidatingStreamingDataWriterFactory(
        StreamingDataWriterFactory delegate, DmeVectorValidator validator) {
      this.delegate = delegate;
      this.validator = validator;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
      return new ValidatingDataWriter(
          delegate.createWriter(partitionId, taskId, epochId), validator);
    }
  }

  private static final class ValidatingDataWriter implements DataWriter<InternalRow> {
    private final DataWriter<InternalRow> delegate;
    private final DmeVectorValidator validator;

    private ValidatingDataWriter(DataWriter<InternalRow> delegate, DmeVectorValidator validator) {
      this.delegate = delegate;
      this.validator = validator;
    }

    @Override
    public void write(InternalRow record) throws IOException {
      validator.validate(record);
      delegate.write(record);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      return delegate.commit();
    }

    @Override
    public void abort() throws IOException {
      delegate.abort();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
