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
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;


public class App {
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

    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject("transformjson-401609");
        options.setRegion("us-east1");
        options.setWorkerRegion("us-east1");
        options.setStagingLocation("gs://sales_data_transform_json/staging/");
        options.setGcpTempLocation("gs://sales_data_transform_json/temp/");
        // parse avro schema from .avsc file
        String avroSchemaStr = null;
        try {
            avroSchemaStr = Arrays.toString(Files.readAllBytes(Paths.get("avro/sale.avsc")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        Pipeline p = Pipeline.create(options);
        p.apply("ReadJSON", TextIO.read().from("gs://sales_data_transform_json/sales_file.json"))
                .apply("ClearJson", ParDo.of(new TrimJsonFn()))
                .apply("Process json", ParDo.of(new processJson()))
                // convert to avro:
                //.apply(ParDo.of(new JsonToAvroDoFn(avroSchemaStr)))
                .apply("WriteToSink", TextIO.write().to("gs://sales_data_transform_json/sales_file1.json"));

        // Execute the pipeline
        PipelineResult result = p.run();
        System.out.println("Pipeline result: " + result.getState());
        PipelineResult.State state = result.waitUntilFinish();
        if (state != PipelineResult.State.DONE) {
            System.out.println("Pipeline failed: " + state);
        }

    }
}
