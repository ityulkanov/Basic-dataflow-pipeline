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

package com.ityulkanov;

import com.google.gson.Gson;
import com.ityulkanov.model.Sale;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.ByteArrayOutputStream;


public class App {
    public static final String SALES_FOLDER = "sales_data_32345543";
    public static final String AVRO_FOLDER = "avro_data_23235343";
    public static final String PROJECT_ID = "transformjson-401609";
    public static final String REGION = "us-east1";

    static class processJson extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String jsonStr = c.element();
            assert jsonStr != null;
            Gson gson = new Gson();
            Sale sale = gson.fromJson(jsonStr, Sale.class);
            sale.setProductName(sale.getProductName().toUpperCase());
            sale.setTransactionID(String.format("%s_%s_%s", sale.getStoreID(), sale.getProductID(), sale.getSalesDate()));
            sale.setUpdatedPrice(sale.getPrice() - sale.getDiscount());
            c.output(gson.toJson(sale));
        }
    }

    public static class TrimJsonFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String rawJson = c.element();
            assert rawJson != null;
            String trimmedJson = rawJson.trim();
            c.output(trimmedJson);
        }
    }

    public static class ConvertToAvro extends DoFn<String, byte[]> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            {
                ReflectDatumWriter<Sale> writer = new ReflectDatumWriter<>(Sale.class);
                DataFileWriter<Sale> dataFileWriter = new DataFileWriter<>(writer);
                Gson gson = new Gson();
                String jsonInput = c.element();
                Sale sale = gson.fromJson(jsonInput, Sale.class);
                // get Schema from class
                Schema schema = ReflectData.get().getSchema(Sale.class);

                // Convert User class to Avro and output
                try {
                    ByteArrayOutputStream avroOutput = new ByteArrayOutputStream();
                    dataFileWriter.create(schema, avroOutput);
                    dataFileWriter.append(sale);
                    c.output(avroOutput.toByteArray());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject(PROJECT_ID);
        options.setRegion(REGION);
        options.setWorkerRegion(REGION);
        options.setStagingLocation("gs://" + SALES_FOLDER + "/staging/");
        options.setGcpTempLocation("gs://" + SALES_FOLDER + "/temp/");
        Pipeline p = Pipeline.create(options);
        p.apply("ReadJSON", TextIO.read().from("gs://" + SALES_FOLDER + "/sales_file.json"))
                .apply("ClearJson", ParDo.of(new TrimJsonFn()))
                .apply("Process json", ParDo.of(new processJson()))
                // convert to avro:
                .apply(ParDo.of(new ConvertToAvro()))
                .apply(
                        "WriteAvroToBucket",
                        AvroIO.write(byte[].class)
                                .to("gs://" + AVRO_FOLDER + "/")
                                .withSuffix(".avro")
                                .withCodec(CodecFactory.snappyCodec()));

        // Execute the pipeline
        PipelineResult result = p.run();
        System.out.println("Pipeline result: " + result.getState());
        PipelineResult.State state = result.waitUntilFinish();
        if (state != PipelineResult.State.DONE) {
            System.out.println("Pipeline failed: " + state);
        }

    }
}
