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
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.CellSpec;

/**
 * Read-modify-write bulk-importer.
 *
 * Allows to read from an existing table and rewrite cells.
 */
public class RMWBulkImporter extends KijiBulkImporter<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(RMWBulkImporter.class);

  /** Configuration key associated to the input table URI. */
  public static final String CONF_KEY_INPUT_TABLE_URI =
      "org.kiji.mapreduce.RMWBulkImporter.INPUT_TABLE_URI";

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  /** {@inheritDoc} */
  @Override
  public void setup(KijiTableContext context) throws IOException {
    super.setup(context);

    final String tableURIStr = getConf().get(CONF_KEY_INPUT_TABLE_URI);
    Preconditions.checkState(tableURIStr != null,
        "Input table URI is missing from Configuration!");
    final KijiURI tableURI = KijiURI.newBuilder(tableURIStr).build();
    LOG.info("Configuring RWMBulkImporter to read from table '{}'.", tableURI);

    mKiji = Kiji.Factory.open(tableURI);
    mTable = mKiji.openTable(tableURI.getTable());

    // The following override map is a dummy example.
    // You must change it according to your specific table and needs:
    final KijiColumnName zipCode = new KijiColumnName("info", "zip_code");
    final Map<KijiColumnName, CellSpec> overrides =
        ImmutableMap.<KijiColumnName, CellSpec>builder()
        .put(zipCode, mTable.getLayout().getCellSpec(zipCode)
            .setReaderSchema(Schema.create(Schema.Type.DOUBLE)))
        .build();

    mReader = mTable.getReaderFactory().openTableReader(overrides);
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup(KijiTableContext context) throws IOException {
    mReader.close();
    mTable.release();
    mKiji.release();
    super.cleanup(context);
  }

  /** {@inheritDoc} */
  @Override
  public void produce(LongWritable inputKey, Text value, KijiTableContext context)
      throws IOException {
    // Input line must be formatted as: "start-key:end-key".
    // The start and end keys are encoded in hexadecimal.
    final String line = value.toString();
    final String[] split = line.split(":", 2);
    Preconditions.checkState(split.length == 2,
        String.format("Unable to process input line specifying the row-key range: '%s'.", line));
    final String hexStartRowKey = split[0];
    final String hexEndRowKey = split[1];

    LOG.info("Scanning table '{}' from start-key: '{}' to end-key: '{}'.",
        mTable.getURI(), hexStartRowKey, hexEndRowKey);

    final EntityId startRowKey =
        HBaseEntityId.fromHBaseRowKey(StringUtils.hexStringToByte(hexStartRowKey));
    final EntityId endRowKey =
        HBaseEntityId.fromHBaseRowKey(StringUtils.hexStringToByte(hexEndRowKey));

    // This data request must be adapted to your table:
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily("info"))
        .build();

    final KijiRowScanner scanner = mReader.getScanner(
        dataRequest,
        new KijiScannerOptions()
            .setStartRow(startRowKey)
            .setStopRow(endRowKey));
    try {
      for (KijiRowData inputRow : scanner) {
        LOG.debug("Processing input row with key '{}'.", inputRow.getEntityId());
        processInputRow(inputRow, context);

        // Report progress to the task tracker:
        // this is necessary to prevent the task tracker from killing the task by timeout.
        context.progress();
      }

    } finally {
      scanner.close();
    }
  }

  /**
   * Processes one input row.
   *
   * @param inputRow Input row to process.
   * @param context Bulk-importer context to write back to the table.
   * @throws IOException on I/O error.
   */
  private void processInputRow(KijiRowData inputRow, KijiTableContext context) throws IOException {
    // This is a dummy example and must be changed according to your specific needs:
    final EntityId eid = inputRow.getEntityId();
    final Double zipCode = inputRow.getMostRecentValue("info", "zip_code");
    context.put(eid, "info", "first_name", Double.toString(zipCode));
  }
}