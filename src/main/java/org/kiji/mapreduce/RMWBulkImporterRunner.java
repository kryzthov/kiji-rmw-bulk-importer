/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.KijiRowKeySplitter;
import org.kiji.schema.KijiURI;

/**
 * Runs a Read-Modify-Write bulk-importer job.
 * This is an example and must be adapted according to your specific needs.
 */
public class RMWBulkImporterRunner {
  private static final Logger LOG = LoggerFactory.getLogger(RMWBulkImporterRunner.class);

  private final KijiURI TABLE_URI =
      KijiURI.newBuilder("kiji://localhost:2181/default").build();
  private final String HDFS_ADDRESS = "hdfs://localhost:8020";
  private final String JOB_TRACKER_ADDRESS = "localhost:8021";

  // -----------------------------------------------------------------------------------------------

  private void run() throws Exception {
    // Setup configuration with:
    //  - HDFS address,
    //  - MapReduce job tracker address,
    //  - and input table URI.
    final Configuration conf = HBaseConfiguration.create();
    conf.set(RMWBulkImporter.CONF_KEY_INPUT_TABLE_URI, TABLE_URI.toString());
    conf.set("fs.defaultFS", HDFS_ADDRESS);
    conf.set("mapred.job.tracker", JOB_TRACKER_ADDRESS);

    // Generate input files, by default, one per map task:
    final int nmappers = 4;
    final int nsplits = nmappers;
    final Path[] inputPaths = generateInputFiles(conf, nsplits).toArray(new Path[]{});

    // Runs the bulk-import:
    final KijiMapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(RMWBulkImporter.class)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(inputPaths))
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(TABLE_URI))
        .build();
    Preconditions.checkState(job.run());
  }

  /**
   * Generates a set of text input files to drive the bulk-importer map tasks.
   *
   * <p>
   * Each input file contains a single entry formatted as:
   *     <code>start-row-key:end-row-key</code>
   * The row keys are encoded in hexadecimal form.
   * </p>
   *
   * <p>
   * This splits the row key space evenly, assuming that the KijiTable uses row key hash-prefixing.
   * </p>
   *
   * @param conf Configuration with the appropriate HDFS URI.
   * @param nsplits Number of splits (input file) to generates.
   * @return the list of input text files to drive the bulk-importer map tasks.
   * @throws IOException on I/O error.
   */
  private List<Path> generateInputFiles(Configuration conf, int nsplits) throws IOException {
    final List<Path> inputPaths = Lists.newArrayList();

    /** Precision of the row-key boundaries, in bytes. */
    final int precision = 4;  // 4 bytes, ie. 8 hex digits
    final byte[][] boundaries = KijiRowKeySplitter.get().getSplitKeys(nsplits, precision);

    byte[] splitStartRowKey = new byte[]{};
    for (byte[] splitEndRowKey : boundaries) {
      final Path path = new Path(HDFS_ADDRESS,
          String.format("/RMWBulkImporter.input.%s.txt", inputPaths.size()));
      inputPaths.add(path);
      final FSDataOutputStream os = FileSystem.get(conf).create(path);
      final PrintWriter writer = new PrintWriter(os);
      writer.printf("%s:%s\n",
          StringUtils.byteToHexString(splitStartRowKey),
          StringUtils.byteToHexString(splitEndRowKey));
      writer.close();
      os.close();

      splitStartRowKey = splitEndRowKey;
    }
    // Write last split:
    final Path path = new Path(HDFS_ADDRESS,
        String.format("/RMWBulkImporter.input.%s.txt", inputPaths.size()));
    inputPaths.add(path);
    final FSDataOutputStream os = FileSystem.get(conf).create(path);
    final PrintWriter writer = new PrintWriter(os);
    writer.printf("%s:%s\n",
        StringUtils.byteToHexString(splitStartRowKey),
        StringUtils.byteToHexString(new byte[]{}));
    writer.close();
    os.close();

    return inputPaths;
  }

  // -----------------------------------------------------------------------------------------------

  public static void main(String[] args) throws Exception {
    new RMWBulkImporterRunner().run();
  }
}
