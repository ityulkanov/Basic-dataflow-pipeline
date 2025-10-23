package com.ityulkanov.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.FieldList;
import com.ityulkanov.cons.IBTConstants;
import com.ityulkanov.dataflow.options.CombinedJobCommonOptions;
import com.ityulkanov.util.CloudStorageUtil;
import com.ityulkanov.util.ErrorTblDtl;
import com.ityulkanov.util.PipelineUtil;
import com.ityulkanov.util.PropertiesFileReader;
import lombok.NonNull;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Properties;

import static com.ityulkanov.cons.IBTConstants.EVENT_DATE_FIELD;
import static com.ityulkanov.cons.IBTConstants.SOURCE_CODE;



public class IBTTransform extends PTransform<PCollection<PubsubMessage>, PDone> {

    static final TupleTag<String> recordTag = new TupleTag<>() {
    };

    static final TupleTag<ErrorTblDtl> transformFail = new TupleTag<>() {
    };
    static final TupleTag<TableRow> transformOut = new TupleTag<>() {
    };
    private static final TupleTag<ErrorTblDtl> transformFailTl = new TupleTag<>() {
    };

    private final CombinedJobCommonOptions options;
    private final Properties properties;


    public IBTTransform(CombinedJobCommonOptions options,
                        Properties properties) {
        this.options = options;
        this.properties = properties;
    }

    @Override
    @NonNull
    public PDone expand(PCollection<PubsubMessage> msgCollection) {

        String outputDirectory = properties.getProperty("ibtAvroOutput");
        String outputDirectoryGS = CloudStorageUtil.getBucketName(options, outputDirectory);
        PipelineUtil.saveAvroFiles(msgCollection, outputDirectoryGS, options);

        PCollectionTuple jsonMessages = PipelineUtil.convertToJson(
                msgCollection,
                recordTag,
                transformFail,
                outputDirectoryGS,
                SOURCE_CODE,
                EVENT_DATE_FIELD
        );

        if (jsonMessages.has(recordTag)) {
            FieldList fieldsCleansed = BigQueryUtil.getTblFields(
                    options.getProject(),
                    options.getDataSet().get(),
                    IBTConstants.TBL_IBT
            );

            PipelineUtil.saveToCleansedLayer(
                    jsonMessages,
                    recordTag,
                    transformFail,
                    transformOut,
                    options,
                    IBTConstants.TBL_IBT,
                    fieldsCleansed
            );
            FieldList fieldsTrusted = BigQueryUtil.getTblFields(
                    options.getProject(),
                    options.getTrustDataSet().get(),
                    IBTConstants.TBL_IBT
            );
            Properties fieldsMapping = PropertiesFileReader.readProperties(IBTConstants.FIELDS_MAPPING);
            jsonMessages.apply(
                    "Insert IBT Trusted Records to BQ",
                    new IBTTrustedTransform(
                            recordTag,
                            options.getProject(),
                            options.getTrustDataSet().get(),
                            IBTConstants.TBL_IBT,
                            options.getTrustErrTbl().get(),
                            fieldsTrusted,
                            fieldsMapping));

        }
        PipelineUtil.processErrors(
                jsonMessages,
                transformFail,
                transformFailTl,
                options.getTrustErrTbl().get(),
                options
        );
        return PDone.in(msgCollection.getPipeline());
    }
}
