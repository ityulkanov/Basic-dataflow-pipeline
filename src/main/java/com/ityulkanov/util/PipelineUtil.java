package com.ityulkanov.util;




import com.google.api.services.bigquery.model.TableRow;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.dataflow.options.CombinedJobCommonOptions;
import com.ityulkanov.transform.AvroTransformV2;
import com.ityulkanov.transform.BigQueryInsertFailedFn;
import com.ityulkanov.transform.InsertBqTransform;
import com.ityulkanov.transform.PubsubMsgToTopLevelUnnestJsonDoFn;
import com.ityulkanov.transform.PubsubMsgToUnnestJsonDoFn;
import com.ityulkanov.transform.SingleEncodePubsubMsgToJsonDoFn;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import com.google.cloud.bigquery.FieldList;

import java.util.List;
import java.util.Properties;

import static com.ityulkanov.transform.BigQueryUtil.configureBigQueryIO;


@Slf4j
public class PipelineUtil {
    private PipelineUtil() {
    }

    public static PCollection<PubsubMessage> readMessagesFromSubscription(
            Pipeline pipeline,
            String transformName,
            String project,
            String subscription) {
        return pipeline
                .apply(
                        transformName,
                        PubsubIO
                                .readMessagesWithAttributes()
                                .fromSubscription(PubSubUtil.getFullSubscriptionName(project, subscription))
                );
    }

    public static void saveAvroFiles(
            PCollection<PubsubMessage> msgCollection,
            String outputDirectory,
            CombinedJobCommonOptions options
    ) {
        msgCollection.apply(
                ContentInfo.AVRO_PTRANSFORM,
                new AvroTransformV2(
                        outputDirectory,
                        options
                )
        );
    }

    public static PCollectionTuple convertToJson(
            PCollection<PubsubMessage> msgCollection,
            TupleTag<String> recordTag,
            TupleTag<ErrorTblDtl> transformFail,
            String outputDirectory,
            String sourceCodeField,
            String eventDateTimeField
    ) {
        return msgCollection
                .apply(
                        ContentInfo.INFO_DECODE_TO_JSON_STRING,
                        ParDo.of(
                                        new SingleEncodePubsubMsgToJsonDoFn(
                                                sourceCodeField,
                                                eventDateTimeField,
                                                recordTag,
                                                transformFail,
                                                outputDirectory
                                        )
                                )
                                .withOutputTags(recordTag, TupleTagList.of(transformFail)));
    }

    public static PCollectionTuple convertToUnnestedJson(
            PCollection<PubsubMessage> msgCollection,
            List<String> outerObjectNames,
            TupleTag<String> recordTag,
            TupleTag<ErrorTblDtl> transformFail,
            String outputDirectory,
            String sourceCodeField,
            String eventDateTimeField
    ) {
        return msgCollection
                .apply(
                        ContentInfo.INFO_DECODE_TO_JSON_STRING,
                        ParDo.of(
                                        new PubsubMsgToUnnestJsonDoFn(
                                                outerObjectNames,
                                                sourceCodeField,
                                                eventDateTimeField,
                                                recordTag,
                                                transformFail,
                                                outputDirectory
                                        )
                                )
                                .withOutputTags(recordTag, TupleTagList.of(transformFail)));
    }
    
    public static PCollectionTuple convertToTopLevelUnnestedJson(
            PCollection<PubsubMessage> msgCollection,
            TupleTag<String> recordTag,
            TupleTag<ErrorTblDtl> transformFail,
            String outputDirectory,
            String sourceCodeField,
            String eventDateTimeField
    ) {
        return msgCollection
                .apply(
                        ContentInfo.INFO_DECODE_TO_JSON_STRING,
                        ParDo.of(
                                new PubsubMsgToTopLevelUnnestJsonDoFn(
                                        sourceCodeField,
                                        eventDateTimeField,
                                        recordTag,
                                        transformFail,
                                        outputDirectory
                                )
                        )
                                .withOutputTags(recordTag, TupleTagList.of(transformFail)));
    }

    public static void saveToCleansedLayer(
            PCollectionTuple jsonMessages,
            TupleTag<String> recordTag,
            TupleTag<ErrorTblDtl> transformFail,
            TupleTag<TableRow> transformOut,
            CombinedJobCommonOptions options,
            String table,
            FieldList fields
    ) {
        saveToBQ(
                jsonMessages,
                recordTag,
                transformFail,
                transformOut,
                options.getProject(),
                options.getDataSet().get(),
                options.getErrTableSpec().get(),
                table,
                fields,
                new Properties()
        );
    }

    public static void saveToTrustedLayerWithNewFieldsNames(
            PCollectionTuple jsonMessages,
            TupleTag<String> recordTag,
            TupleTag<ErrorTblDtl> transformFailTl,
            TupleTag<TableRow> transformOut,
            CombinedJobCommonOptions options,
            String table,
            FieldList fields,
            Properties properties) {
        saveToBQ(
                jsonMessages,
                recordTag,
                transformFailTl,
                transformOut,
                options.getProject(),
                options.getTrustDataSet().get(),
                options.getTrustErrTbl().get(),
                table,
                fields,
                properties
        );
    }

    private static void saveToBQ(
            PCollectionTuple jsonMessages,
            TupleTag<String> recordTag,
            TupleTag<ErrorTblDtl> transformFailTl,
            TupleTag<TableRow> transformOut,
            String project,
            String dataset,
            String errTable,
            String table,
            FieldList fields,
            Properties properties) {
        jsonMessages.apply(
                ContentInfo.BQ_INSERT_TRANSFORM,
                new InsertBqTransform(
                        recordTag,
                        transformOut,
                        transformFailTl,
                        project,
                        dataset,
                        table,
                        errTable,
                        fields,
                        properties
                )
        );
    }

    public static void processErrors(
            PCollectionTuple jsonMessages,
            TupleTag<ErrorTblDtl> transformFail,
            TupleTag<ErrorTblDtl> transformFailTl,
            String trustedErrTbl,
            CombinedJobCommonOptions options) {
        if (jsonMessages.has(transformFail)) {
            String errTbl = options.getErrTableSpec().get();
            insertErrorRecord(jsonMessages, transformFail, errTbl);
        }
        if (jsonMessages.has(transformFailTl)) {
            insertErrorRecord(jsonMessages, transformFailTl, trustedErrTbl);
        }
    }

    private static void insertErrorRecord(
            PCollectionTuple jsonMessages,
            TupleTag<ErrorTblDtl> transformFailGen,
            String errTbl) {
        try {
            jsonMessages
                    .get(transformFailGen)
                    .apply(
                            ContentInfo.INFO_CREATE_FAILED_RECORD,
                            ParDo.of(new BigQueryInsertFailedFn())
                    )
                    .apply(
                            ContentInfo.INFO_WRITE_FAILED_RECORD,
                            BigQueryIO
                                    .writeTableRows()
                                    .to(errTbl)
                                    .withoutValidation()
                                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                    .withExtendedErrorInfo()
                                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                    .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));
        } catch (Exception e) {
            log.error("{} : {}", ContentInfo.INFO_ERR_TAB_INSERT_FAILED, ExceptionUtil.getExceptionAsString(e));
        }
    }

    public static void writeToErrTable(PCollectionTuple collectionTuple, TupleTag<ErrorTblDtl> tupleTag, String errorTable) {
        collectionTuple.get(tupleTag)
                .apply(ContentInfo.INFO_WRITE_JSON_TO_ERR_TABLE, ParDo.of(new BigQueryInsertFailedFn()))
                .apply(ContentInfo.INFO_WRITE_FAILED_REC, configureBigQueryIO(errorTable));
    }
}
