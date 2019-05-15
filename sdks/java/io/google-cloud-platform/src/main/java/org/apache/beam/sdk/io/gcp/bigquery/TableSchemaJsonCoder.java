/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.beam.sdk.io.gcp.bigquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link org.apache.beam.sdk.coders.Coder} that encodes BigQuery {@link TableSchema} objects as
 * JSON.
 */
public class TableSchemaJsonCoder extends AtomicCoder<TableSchema> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TableSchemaJsonCoder INSTANCE = new TableSchemaJsonCoder();

  public static TableSchemaJsonCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableSchema value, OutputStream outStream) throws CoderException, IOException {
    String strValue = MAPPER.writeValueAsString(value);
    StringUtf8Coder.of().encode(strValue, outStream);
  }

  @Override
  public TableSchema decode(InputStream inStream) throws CoderException, IOException {
    String strValue = StringUtf8Coder.of().decode(inStream);
    return MAPPER.readValue(strValue, TableSchema.class);
  }
}
