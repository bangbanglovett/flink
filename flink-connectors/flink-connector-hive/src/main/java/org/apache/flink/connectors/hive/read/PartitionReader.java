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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.annotation.Internal;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Reader that reads all records from given partitions.
 *
 *<P>This reader should only use in non-parallel instance, e.g. : used by lookup function.
 *
 * @param <P> The type of partition.
 * @param <OUT> The type of returned record.
 */
@Internal
public interface PartitionReader<P, OUT> extends Closeable, Serializable {

	/**
	 * Opens the reader with given partitions.
	 * @throws IOException
	 */
	void open(List<P> partitions) throws IOException;

	/**
	 * Method used to check the partitions have read finished or not.
	 *
	 *<p>When this method is called, the reader it guaranteed to be opened.
	 *
	 * @return True if the partitions has read finished.
	 * @throws IOException
	 */
	boolean hasNext() throws IOException;

	/**
	 * Reads the next record from the partitions.
	 *
	 * <p>When this method is called, the reader it guaranteed to be opened.
	 *
	 * @param reuse Object that may be reused.
	 * @return Read record.
	 * @throws IOException
	 */
	OUT nextRecord(OUT reuse) throws IOException;

	/**
	 * Close the reader, this method should release all resources.
	 *
	 *<p>When this method is called, the reader it guaranteed to be opened.
	 *
	 * @throws IOException
	 */
	@Override
	void close() throws IOException;
}
