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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowKeySplitter;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;

/** Runs a bulk-importer job in-process against a fake HBase instance. */
public class TestRMWBulkImporter extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestRMWBulkImporter.class);

  private static final String TABLE_NAME = "test";
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  private final boolean inProcess = true;
  private final KijiURI externalTableURI =
      KijiURI.newBuilder("kiji://localhost:2181/default").build();
  private final String externalJobTracker = "localhost:8021";

  @Before
  public final void setupTestBulkImporter() throws Exception {
    final KijiURI kijiURI = inProcess ? getKiji().getURI() : externalTableURI;

    mKiji = Kiji.Factory.open(kijiURI);
    mKiji.createTable(KijiTableLayouts.getLayout("layout/TestRMWBulkImporter.layout.json"));
    mTable = mKiji.openTable(TABLE_NAME);

    // Write some rows:
    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      for (int i = 0; i < 10; ++i) {
        final EntityId entityId = mTable.getEntityId("entity-" + i);
        writer.put(entityId, "info", "zip_code", i);
      }
    } finally {
      writer.close();
    }

    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestBulkImporter() throws Exception {
    mReader.close();
    mReader = null;

    mTable.release();
    mTable = null;

    mKiji.release();
    mKiji = null;
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testRMWBulkImporter() throws Exception {
    // Generate input file:
    final int nmappers = 4;
    final int nsplits = nmappers;
    final File inputFile = generateInputFile(nsplits);

    final Configuration conf = getConf();
    conf.set(RMWBulkImporter.CONF_KEY_INPUT_TABLE_URI, mTable.getURI().toString());
    conf.set("mapred.job.tracker", inProcess ? "local" : externalJobTracker);

    // Run the bulk-import:
    final KijiMapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(RMWBulkImporter.class)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Log output:
    final KijiRowScanner scanner = mReader.getScanner(KijiDataRequest.create("info"));
    try {
      for (KijiRowData row : scanner) {
        LOG.info("Row ID={} info:first_name={} info:zip_code={}",
            row.getEntityId(),
            row.getMostRecentValue("info", "first_name"),
            row.getMostRecentValue("info", "zip_code"));
      }
    } finally {
      scanner.close();
    }
  }

  private File generateInputFile(int nsplits) throws IOException {
    /** Precision of the row-key boundaries, in bytes. */
    final int precision = 4;  // 4 bytes, ie. 8 hex digits

    final byte[][] boundaries = KijiRowKeySplitter.get().getSplitKeys(nsplits, precision);

    final File inputFile = File.createTempFile("TestBulkImportInput", ".txt", getLocalTempDir());
    final PrintWriter writer = new PrintWriter(inputFile);

    byte[] splitStartRowKey = new byte[]{};
    for (byte[] splitEndRowKey : boundaries) {
      writer.printf("%s:%s\n",
          StringUtils.byteToHexString(splitStartRowKey),
          StringUtils.byteToHexString(splitEndRowKey));

      splitStartRowKey = splitEndRowKey;
    }
    // Write last split:
    writer.printf("%s:%s\n",
        StringUtils.byteToHexString(splitStartRowKey),
        StringUtils.byteToHexString(new byte[]{}));

    writer.close();
    return inputFile;
  }
}
