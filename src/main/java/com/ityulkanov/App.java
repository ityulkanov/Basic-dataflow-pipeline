package com.ityulkanov;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.ityulkanov.funcs.AvroToTableRow;
import com.ityulkanov.funcs.ByteArrayToUser;
import com.ityulkanov.funcs.ConvertToAvro;
import com.ityulkanov.funcs.ProcessJson;
import com.ityulkanov.funcs.TrimJson;
import com.ityulkanov.model.Sale;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;


public class App {
    public static final String SALES_FOLDER = "sales_data_32345543";
    public static final String AVRO_FOLDER = "avro_data_23235343";
    public static final String PROJECT_ID = "transformjson-401609";
    public static final String REGION = "us-east1";
    public static final String DATASET_NAME = "avro_dataset_9494959";
    public static final String TABLE_NAME = "avro_data_table_9494959";


    public static void main(String[] args) {
        Pipeline p = setupPipeline();
        p.apply("ReadJSON", TextIO.read().from("gs://" + SALES_FOLDER + "/sales_file.json"))
                .apply("ClearJSON", ParDo.of(new TrimJson()))
                .apply("ProcessJSON", ParDo.of(new ProcessJson()))
                .apply(ParDo.of(new ConvertToAvro()))
                .apply(
                        "WriteAVROToBucket",
                        AvroIO.write(byte[].class)
                                .to("gs://" + AVRO_FOLDER + "/")
                                .withSuffix(".avro")
                                .withCodec(CodecFactory.snappyCodec()));
        PipelineResult result = p.run();
        System.out.println("Pipeline result: " + result.getState());
        PipelineResult.State state = result.waitUntilFinish();
        if (state != PipelineResult.State.DONE) {
            System.out.println("Pipeline failed: " + state);
        }

        Pipeline p2 = setupPipeline();
        // trying to solve the missing coder issue here
        CoderRegistry coderRegistry = p2.getCoderRegistry();
        coderRegistry.registerCoderForClass(Sale.class, AvroCoder.of(Sale.class));
        TableSchema bigQuerySchema = getBigQuerySchema();
        p2.apply("ReadAVRO", AvroIO.read(byte[].class).from("gs://" + AVRO_FOLDER + "/*.avro"))
                .apply("ConvertToInternalClas", ParDo.of(new ByteArrayToUser()))
                .apply("InternalClassToTableRow", ParDo.of(new AvroToTableRow()))
                .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                        .to(DATASET_NAME + ":" + TABLE_NAME)
                        .withSchema(bigQuerySchema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        // Execute the second pipeline
        PipelineResult secondPipelineResult = p2.run();
        System.out.println("Second pipeline result: " + secondPipelineResult.getState());
        PipelineResult.State secondPipelineState = secondPipelineResult.waitUntilFinish();
        if (secondPipelineState != PipelineResult.State.DONE) {
            System.out.println("Second pipeline failed: " + secondPipelineState);
        }
    }

    private static Pipeline setupPipeline() {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject(PROJECT_ID);
        options.setRegion(REGION);
        options.setWorkerRegion(REGION);
        options.setStagingLocation("gs://" + SALES_FOLDER + "/staging/");
        options.setGcpTempLocation("gs://" + SALES_FOLDER + "/temp/");
        Pipeline p = Pipeline.create(options);
        return p;
    }

    private static TableSchema getBigQuerySchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("sales_date").setType("STRING"),
                new TableFieldSchema().setName("store_id").setType("STRING"),
                new TableFieldSchema().setName("product_id").setType("STRING"),
                new TableFieldSchema().setName("product_name").setType("STRING"),
                new TableFieldSchema().setName("price").setType("FLOAT"),
                new TableFieldSchema().setName("discount").setType("FLOAT"),
                new TableFieldSchema().setName("updated_price").setType("FLOAT"),
                new TableFieldSchema().setName("transaction_id").setType("STRING")

        ));
    }
}
