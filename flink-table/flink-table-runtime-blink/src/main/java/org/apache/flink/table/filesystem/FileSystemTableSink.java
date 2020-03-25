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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * File system {@link TableSink}.
 */
public class FileSystemTableSink implements AppendStreamTableSink<BaseRow>, PartitionableTableSink, OverwritableTableSink {

	private final TableSchema schema;
	private final boolean isStreaming;
	private final List<String> partitionKeys;
	private final Path path;
	private final String defaultPartName;
	private final FileSystemFormatFactory formatFactory;

	private boolean overwrite = false;
	private boolean dynamicGrouping = false;
	private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();

	public FileSystemTableSink(
			TableSchema schema,
			Path path,
			List<String> partitionKeys,
			boolean isStreaming,
			String defaultPartName,
			FileSystemFormatFactory formatFactory) {
		this.schema = schema;
		this.path = path;
		this.defaultPartName = defaultPartName;
		this.formatFactory = formatFactory;
		this.isStreaming = isStreaming;
		this.partitionKeys = partitionKeys;
	}

	@Override
	public final DataStreamSink<BaseRow> consumeDataStream(DataStream<BaseRow> dataStream) {
		BaseRowPartitionComputer computer = new BaseRowPartitionComputer(
				defaultPartName,
				schema.getFieldNames(),
				schema.getFieldDataTypes(),
				partitionKeys.toArray(new String[0]));
		if (!isStreaming) {
			FileSystemOutputFormat.Builder<BaseRow> builder = new FileSystemOutputFormat.Builder<>();
			builder.setPartitionComputer(computer);
			builder.setDynamicGrouped(dynamicGrouping);
			builder.setPartitionColumns(partitionKeys.toArray(new String[0]));
			builder.setFormatFactory(createOutputFormatFactory(
					formatFactory, this::getTypesWithoutPart));
			builder.setMetaStoreFactory(createTableMetaStoreFactory(path));
			builder.setOverwrite(overwrite);
			builder.setStaticPartitions(staticPartitions);
			builder.setTempPath(toStagingPath());
			return dataStream.writeUsingOutputFormat(builder.build())
					.setParallelism(dataStream.getParallelism());
		} else {
			throw new UnsupportedOperationException();
		}
	}

	private DataType[] getTypesWithoutPart() {
		return Arrays.stream(schema.getFieldNames())
				.filter(name -> !partitionKeys.contains(name))
				.map(name -> schema.getFieldDataType(name).get())
				.toArray(DataType[]::new);
	}

	private Path toStagingPath() {
		Path stagingDir = new Path(path, ".staging_" + System.currentTimeMillis());
		try {
			FileSystem fs = stagingDir.getFileSystem();
			Preconditions.checkState(
					fs.exists(stagingDir) || fs.mkdirs(stagingDir),
					"Failed to create staging dir " + stagingDir);
			return stagingDir;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static OutputFormatFactory<BaseRow> createOutputFormatFactory(
			FileSystemFormatFactory formatFactory,
			FileSystemFormatFactory.WriterContext context) {
		Optional<Encoder<BaseRow>> encoder = formatFactory.createEncoder(context);
		Optional<BulkWriter.Factory<BaseRow>> bulk = formatFactory.createBulkWriterFactory(context);
		return encoder.<OutputFormatFactory<BaseRow>>map(
				baseRowEncoder -> path -> createEncoderOutputFormat(baseRowEncoder, path))
				.orElseGet(() -> path -> createBulkWriterOutputFormat(bulk.get(), path));
	}

	private static OutputFormat<BaseRow> createBulkWriterOutputFormat(
			BulkWriter.Factory<BaseRow> factory,
			Path path) {
		return new OutputFormat<BaseRow>() {

			private BulkWriter<BaseRow> writer;

			@Override
			public void configure(Configuration parameters) {
			}

			@Override
			public void open(int taskNumber, int numTasks) throws IOException {
				this.writer = factory.create(path.getFileSystem()
						.create(path, FileSystem.WriteMode.OVERWRITE));
			}

			@Override
			public void writeRecord(BaseRow record) throws IOException {
				writer.addElement(record);
			}

			@Override
			public void close() throws IOException {
				writer.flush();
				writer.finish();
			}
		};
	}

	private static OutputFormat<BaseRow> createEncoderOutputFormat(
			Encoder<BaseRow> encoder,
			Path path) {
		return new OutputFormat<BaseRow>() {

			private FSDataOutputStream output;

			@Override
			public void configure(Configuration parameters) {
			}

			@Override
			public void open(int taskNumber, int numTasks) throws IOException {
				this.output = path.getFileSystem()
						.create(path, FileSystem.WriteMode.OVERWRITE);
			}

			@Override
			public void writeRecord(BaseRow record) throws IOException {
				encoder.encode(record, output);
			}

			@Override
			public void close() throws IOException {
				this.output.flush();
				this.output.close();
			}
		};
	}

	private static TableMetaStoreFactory createTableMetaStoreFactory(Path path) {
		return (TableMetaStoreFactory) () -> new TableMetaStoreFactory.TableMetaStore() {

			@Override
			public Path getLocationPath() {
				return path;
			}

			@Override
			public Optional<Path> getPartition(LinkedHashMap<String, String> partitionSpec) {
				return Optional.empty();
			}

			@Override
			public void createPartition(
					LinkedHashMap<String, String> partitionSpec,
					Path partitionPath) {
			}

			@Override
			public void close() {
			}
		};
	}

	@Override
	public FileSystemTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return this;
	}

	@Override
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	@Override
	public void setStaticPartition(Map<String, String> partitions) {
		this.staticPartitions = toLinkedPartSpec(partitions);
	}

	private LinkedHashMap<String, String> toLinkedPartSpec(Map<String, String> part) {
		LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
		for (String partitionKey : partitionKeys) {
			if (part.containsKey(partitionKey)) {
				partSpec.put(partitionKey, part.get(partitionKey));
			}
		}
		return partSpec;
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public DataType getConsumedDataType() {
		return schema.toRowDataType().bridgedTo(BaseRow.class);
	}

	@Override
	public boolean configurePartitionGrouping(boolean supportsGrouping) {
		this.dynamicGrouping = !isStreaming && supportsGrouping;
		return dynamicGrouping;
	}
}
