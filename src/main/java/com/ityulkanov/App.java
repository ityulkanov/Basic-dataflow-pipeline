package com.ityulkanov;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.FieldList;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.dataflow.options.CombinedJobCommonOptions;
import com.ityulkanov.transform.BigQueryUtil;
import com.ityulkanov.transform.IbtTrustedTransform;
import com.ityulkanov.util.CloudStorageUtil;
import com.ityulkanov.util.ErrorTblDtl;
import com.ityulkanov.util.PipelineUtil;
import com.ityulkanov.util.PropertiesFileReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@Slf4j
public class App {
    public interface Options extends CombinedJobCommonOptions {

    }

//    public static final String STRING = "STRING";
//    public static final String FLOAT = "FLOAT";

    private static final String SOURCE_CODE = "isl";
    private static final String EVENT_DATE_FIELD = "eventTimestamp";
    private static final String PROPERTIES = "general.properties";
    private static final String TABLE = "tbl_isl_ibt";
    private static final String FIELDS_MAPPING = "bq_fields_mapping/ibt.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private static final TupleTag<String> recordTag = new TupleTag<>() {};
    private static final TupleTag<TableRow> transformOut = new TupleTag<>() {};
    private static final TupleTag<ErrorTblDtl> transformFail = new TupleTag<>() {};
    private static final TupleTag<ErrorTblDtl> transformFailTl = new TupleTag<>() {};

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(Options.class);
        options.setStreaming(true);
        runPipeline(options);

    }

    private static void runPipeline(Options options) {
        Pipeline pipeline = Pipeline.create(options);
        Properties properties = PropertiesFileReader.readProperties(PROPERTIES);
        String project = options.getProject();
        String subscription = options.getSubscription();

        PCollection<PubsubMessage> messages = PipelineUtil.readMessagesFromSubscription(
                pipeline,
                ContentInfo.INFO_READ_PUBSUB_EVENTS,
                project,
                subscription
        );

        String outputDirectory = properties.getProperty("ibtAvroOutput");
        String outputDirectoryGS = CloudStorageUtil.getBucketName(options, outputDirectory);
        PipelineUtil.saveAvroFiles(messages, outputDirectoryGS, options);

        PCollectionTuple jsonMessages = PipelineUtil.convertToJson(
                messages,
                recordTag,
                transformFail,
                outputDirectoryGS,
                SOURCE_CODE,
                EVENT_DATE_FIELD
        );

        if (jsonMessages.has(recordTag)) {
            // Save to Cleansed Layer
            FieldList fieldsCleansed = BigQueryUtil.getTblFields(
                    options.getProject(),
                    options.getDataSet().get(),
                    TABLE);
            PipelineUtil.saveToCleansedLayer(
                    jsonMessages,
                    recordTag,
                    transformFail,
                    transformOut,
                    options,
                    TABLE,
                    fieldsCleansed);

            // Save to Trusted Layer
            Properties fieldsMapping = PropertiesFileReader.readProperties(FIELDS_MAPPING);
            PCollectionTuple trustedTransformResult = jsonMessages.get(recordTag)
                    .apply(ContentInfo.INFO_IBT_SERVICE_TRUSTED_PTRANSFORM, new IbtTrustedTransform(transformOut, transformFailTl, fieldsMapping));

            if (trustedTransformResult.has(transformOut)) {
                String trustedTableSpec = String.format("%s:%s.%s", options.getProject(), options.getTrustDataSet().get(), TABLE);
                trustedTransformResult.get(transformOut)
                        .apply("Write to Trusted BQ", BigQueryIO.writeTableRows()
                                .to(trustedTableSpec)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withExtendedErrorInfo()
                                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));
            }

            // Merge trusted layer transformation errors with other errors
//            if (trustedTransformResult.has(transformFailTl)) {
//                PipelineUtil.mergeErrorCollections(jsonMessages, trustedTransformResult.get(transformFailTl), transformFailTl);
//            }
        }

        PipelineUtil.processErrors(jsonMessages, transformFail, transformFailTl, options.getTrustErrTbl().get(), options);
        pipeline.run();
    }

//        Config config = Config.LoadConfig(args[0]);
//        log.info("config loaded");
//        log.info("creating avro - bq pipeline");
//        // running a pipeline to read any new file appearing in gs folder and storing it into BQ
//        Pipeline p = setupPipeline(config);
//        CoderRegistry coderRegistry = p.getCoderRegistry();
//        coderRegistry.registerCoderForClass(Sale.class, AvroCoder.of(Sale.class));
//        // creating schema for the bq table
//        TableSchema bigQuerySchema = getBigQuerySchema();
//        PCollection<Sale> readAVRO = p.apply("ReadAVRO", AvroIO.read(Sale.class)
//                .from("gs://" + config.sink.folder + "/*.avro")
//                // watch.Grow is a way to tell the pipeline to wait for the file to be fully written
//                .watchForNewFiles(Duration.standardMinutes(2), Watch.Growth.never()));
//        log.debug("avro file being added to a google storage");
//        PCollection<TableRow> saleBQ = readAVRO.apply("saleToBQFormat", ParDo.of(new SaleToTableRow()));
//        log.debug("avro file being converted to table row");
//        saleBQ.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
//                .to(config.project.projectId + "." + config.sink.BQDataset + "." + config.sink.BQTable)
//                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of("gs://" + config.sink.folder + "/temp/"))
//                .withSchema(bigQuerySchema)
//                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
//        log.debug("avro file being written to bq");
//        PipelineResult secondPipelineResult = p.run();
//        log.info(String.format("avro - bq pipeline started: %s", secondPipelineResult.getState()));
//        // storing a file into gs folder:
//        log.info("Creating json processing pipeline");
//        Pipeline p2 = setupPipeline(config);
//        PCollection<String> json = p2.apply("ReadJSON", TextIO.read().from("gs://" + config.source.folder + "/sales_file.json"));
//        log.debug("json file being read");
//        PCollection<String> clearJSON = json.apply("ClearJSON", ParDo.of(new TrimJson()));
//        log.debug("json file being trimmed");
//        PCollection<Sale> sale = clearJSON.apply("ProcessJSON", ParDo.of(new ProcessJson()));
//        log.debug("json file being converted to internal class");
//        sale.apply("WriteAVROToBucket", AvroIO.write(Sale.class)
//                .to("gs://" + config.sink.folder + "/")
//                .withSuffix(".avro"));
//        log.debug("internal class being written to google storage folder");
//        PipelineResult result = p2.run();
//        log.info(String.format("Json processing pipeline started: %s", result.getState()));
//        PipelineResult.State state = result.waitUntilFinish();
//        if (state != PipelineResult.State.DONE) {
//            log.error("avro - bq failed");
//        }
//    }

//    private static Pipeline setupPipeline(Config config) {
//        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
//        var workerLogLevelOverrides = new DataflowWorkerLoggingOptions.WorkerLogLevelOverrides();
//        workerLogLevelOverrides.addOverrideForPackage(Package.getPackage("com.ityulkanov"), DataflowWorkerLoggingOptions.Level.TRACE);
//        options.setWorkerLogLevelOverrides(workerLogLevelOverrides);
//        options.setRunner(DataflowRunner.class);
//        options.setProject(config.project.projectId);
//        options.setRegion(config.project.projectRegion);
//        options.setWorkerRegion(config.project.projectRegion);
//        options.setStagingLocation("gs://" + config.source.folder + "/staging/");
//        options.setGcpTempLocation("gs://" + config.source.folder + "/temp/");
//        return Pipeline.create(options);
//    }
//
//    private static TableSchema getBigQuerySchema() {
//        return new TableSchema().setFields(Arrays.asList(
//                new TableFieldSchema().setName("sales_date").setType(STRING),
//                new TableFieldSchema().setName("store_id").setType(STRING),
//                new TableFieldSchema().setName("product_id").setType(STRING),
//                new TableFieldSchema().setName("product_name").setType(STRING),
//                new TableFieldSchema().setName("price").setType(FLOAT),
//                new TableFieldSchema().setName("discount").setType(FLOAT),
//                new TableFieldSchema().setName("updated_price").setType(FLOAT),
//                new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
//                new TableFieldSchema().setName("transaction_id").setType(STRING)));
//    }
}
