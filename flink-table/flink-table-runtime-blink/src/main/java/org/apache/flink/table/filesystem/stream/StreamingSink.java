/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.filesystem.FileSystemFactory;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;
import org.apache.flink.table.filesystem.stream.compact.CompactBucketWriter;
import org.apache.flink.table.filesystem.stream.compact.CompactCoordinator;
import org.apache.flink.table.filesystem.stream.compact.CompactFileWriter;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.table.filesystem.stream.compact.CompactOperator;
import org.apache.flink.table.filesystem.stream.compact.CompactReader;
import org.apache.flink.table.filesystem.stream.compact.CompactWriter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;

import static org.apache.flink.table.api.config.TableConfigOptions.LOCAL_TIME_ZONE;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isTimestampWithLocalZone;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;

/** Helper for creating streaming file sink. */
public class StreamingSink {

    private static final ZoneId UTC_ZONE_ID = TimeZone.getTimeZone("UTC").toZoneId();

    private StreamingSink() {}

    /**
     * Create a file writer by input stream. This is similar to {@link StreamingFileSink}, in
     * addition, it can emit {@link PartitionCommitInfo} to down stream.
     */
    public static <T> DataStream<PartitionCommitInfo> writer(
            DataStream<T> inputStream,
            long bucketCheckInterval,
            StreamingFileSink.BucketsBuilder<
                            T, String, ? extends StreamingFileSink.BucketsBuilder<T, String, ?>>
                    bucketsBuilder,
            int parallelism) {
        StreamingFileWriter<T> fileWriter =
                new StreamingFileWriter<>(bucketCheckInterval, bucketsBuilder);
        return inputStream
                .transform(
                        StreamingFileWriter.class.getSimpleName(),
                        TypeInformation.of(PartitionCommitInfo.class),
                        fileWriter)
                .setParallelism(parallelism);
    }

    /**
     * Create a file writer with compaction operators by input stream. In addition, it can emit
     * {@link PartitionCommitInfo} to down stream.
     */
    public static <T> DataStream<PartitionCommitInfo> compactionWriter(
            DataStream<T> inputStream,
            long bucketCheckInterval,
            StreamingFileSink.BucketsBuilder<
                            T, String, ? extends StreamingFileSink.BucketsBuilder<T, String, ?>>
                    bucketsBuilder,
            FileSystemFactory fsFactory,
            Path path,
            CompactReader.Factory<T> readFactory,
            long targetFileSize,
            int parallelism) {
        CompactFileWriter<T> writer = new CompactFileWriter<>(bucketCheckInterval, bucketsBuilder);

        SupplierWithException<FileSystem, IOException> fsSupplier =
                (SupplierWithException<FileSystem, IOException> & Serializable)
                        () -> fsFactory.create(path.toUri());

        CompactCoordinator coordinator = new CompactCoordinator(fsSupplier, targetFileSize);

        SingleOutputStreamOperator<CoordinatorOutput> coordinatorOp =
                inputStream
                        .transform(
                                "streaming-writer",
                                TypeInformation.of(CoordinatorInput.class),
                                writer)
                        .setParallelism(parallelism)
                        .transform(
                                "compact-coordinator",
                                TypeInformation.of(CoordinatorOutput.class),
                                coordinator)
                        .setParallelism(1)
                        .setMaxParallelism(1);

        CompactWriter.Factory<T> writerFactory =
                CompactBucketWriter.factory(
                        (SupplierWithException<BucketWriter<T, String>, IOException> & Serializable)
                                bucketsBuilder::createBucketWriter);

        CompactOperator<T> compacter =
                new CompactOperator<>(fsSupplier, readFactory, writerFactory);

        return coordinatorOp
                .broadcast()
                .transform(
                        "compact-operator",
                        TypeInformation.of(PartitionCommitInfo.class),
                        compacter)
                .setParallelism(parallelism);
    }

    /**
     * Create a sink from file writer. Decide whether to add the node to commit partitions according
     * to options.
     */
    public static DataStreamSink<?> sink(
            DataStream<PartitionCommitInfo> writer,
            Path locationPath,
            ObjectIdentifier identifier,
            List<String> partitionKeys,
            TableMetaStoreFactory msFactory,
            FileSystemFactory fsFactory,
            Configuration options,
            ZoneId rowtimeShiftTimeZone) {
        DataStream<?> stream = writer;
        if (partitionKeys.size() > 0 && options.contains(SINK_PARTITION_COMMIT_POLICY_KIND)) {
            PartitionCommitter committer =
                    new PartitionCommitter(
                            locationPath,
                            identifier,
                            partitionKeys,
                            msFactory,
                            fsFactory,
                            options,
                            rowtimeShiftTimeZone);
            stream =
                    writer.transform(
                                    PartitionCommitter.class.getSimpleName(), Types.VOID, committer)
                            .setParallelism(1)
                            .setMaxParallelism(1);
        }

        return stream.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    /**
     * Returns the shift time zone of the rowtime attribute field if its type is TIMESTAMP_LTZ.
     *
     * <p>Returns UTC time zone for other cases which means the rowtime field hasn't been shifted.
     */
    public static ZoneId getRowtimeShiftTimeZone(
            ReadableConfig config, @Nullable DataType timeAttributeDataType) {
        if (timeAttributeDataType != null
                && isRowtimeAttribute(timeAttributeDataType.getLogicalType())
                && isTimestampWithLocalZone(timeAttributeDataType.getLogicalType())) {
            String zoneId = config.get(LOCAL_TIME_ZONE);
            return LOCAL_TIME_ZONE.defaultValue().equals(zoneId)
                    ? ZoneId.systemDefault()
                    : ZoneId.of(zoneId);
        } else {
            return UTC_ZONE_ID;
        }
    }
}
