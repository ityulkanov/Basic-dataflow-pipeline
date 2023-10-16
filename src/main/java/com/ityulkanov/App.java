package com.ityulkanov;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.ityulkanov.avro.Sale;
import com.ityulkanov.config.Config;
import com.ityulkanov.funcs.ProcessJson;
import com.ityulkanov.funcs.SaleToTableRow;
import com.ityulkanov.funcs.TrimJson;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Arrays;
// TODO do not start streaming pipeline if same one already started
// TODO parse terraform variables from the same json file

@Slf4j
public class App {
    public static void main(String[] args) {
        Config config = Config.LoadConfig(args[0]);
        log.info("config loaded");
        log.info("creating avro - bq pipeline");
        // running a pipeline to read any new file appearing in gs folder and storing it into BQ
        Pipeline p = setupPipeline(config);
        CoderRegistry coderRegistry = p.getCoderRegistry();
        coderRegistry.registerCoderForClass(Sale.class, AvroCoder.of(Sale.class));
        // creating schema for the bq table
        TableSchema bigQuerySchema = getBigQuerySchema();
        PCollection<Sale> readAVRO = p.apply("ReadAVRO", AvroIO.read(Sale.class)
                .from("gs://" + config.sink.folder + "/*.avro")
                // watch.Grow is a way to tell the pipeline to wait for the file to be fully written
                .watchForNewFiles(Duration.standardMinutes(2), Watch.Growth.never()));
        log.debug("avro file being added to a google storage");
        PCollection<TableRow> saleBQ = readAVRO.apply("saleToBQFormat", ParDo.of(new SaleToTableRow()));
        log.debug("avro file being converted to table row");
        saleBQ.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to(config.project.projectId + "." + config.sink.BQDataset + "." + config.sink.BQTable)
                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of("gs://" + config.sink.folder + "/temp/"))
                .withSchema(bigQuerySchema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        log.debug("avro file being written to bq");
        PipelineResult secondPipelineResult = p.run();
        log.info(String.format("avro - bq pipeline started: %s", secondPipelineResult.getState()));
        // storing a file into gs folder:
        log.info("Creating json processing pipeline");
        Pipeline p2 = setupPipeline(config);
        PCollection<String> json = p2.apply("ReadJSON", TextIO.read().from("gs://" + config.source.folder + "/sales_file.json"));
        log.debug("json file being read");
        PCollection<String> clearJSON = json.apply("ClearJSON", ParDo.of(new TrimJson()));
        log.debug("json file being trimmed");
        PCollection<Sale> sale = clearJSON.apply("ProcessJSON", ParDo.of(new ProcessJson()));
        log.debug("json file being converted to internal class");
        sale.apply("WriteAVROToBucket", AvroIO.write(Sale.class)
                .to("gs://" + config.sink.folder + "/")
                .withSuffix(".avro"));
        log.debug("internal class being written to google storage folder");
        PipelineResult result = p2.run();
        log.info(String.format("Json processing pipeline started: %s", result.getState()));
        PipelineResult.State state = result.waitUntilFinish();
        if (state != PipelineResult.State.DONE) {
            log.error("avro - bq failed");
        }
    }

    private static Pipeline setupPipeline(Config config) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject(config.project.projectId);
        options.setRegion(config.project.projectRegion);
        options.setWorkerRegion(config.project.projectRegion);
        options.setStagingLocation("gs://" + config.source.folder + "/staging/");
        options.setGcpTempLocation("gs://" + config.source.folder + "/temp/");
        return Pipeline.create(options);
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
                new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
                new TableFieldSchema().setName("transaction_id").setType("STRING")));
    }
}
