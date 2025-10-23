package com.ityulkanov;

import com.google.api.services.bigquery.model.TableRow;
import com.ityulkanov.dataflow.options.CombinedJobCommonOptions;
import com.ityulkanov.transform.IBTTransform;
import com.ityulkanov.util.ErrorTblDtl;
import com.ityulkanov.util.PipelineUtil;
import com.ityulkanov.util.PropertiesFileReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Properties;

import static com.ityulkanov.cons.IBTConstants.GENERAL_PROPERTIES;


@Slf4j
public class App {
    public interface Options extends CombinedJobCommonOptions {
    }


    


    private static final TupleTag<String> recordTag = new TupleTag<>() {
    };
    private static final TupleTag<TableRow> transformOut = new TupleTag<>() {
    };
    private static final TupleTag<ErrorTblDtl> transformFail = new TupleTag<>() {
    };
    private static final TupleTag<ErrorTblDtl> transformFailTl = new TupleTag<>() {
    };

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
        Properties props = PropertiesFileReader.readProperties(GENERAL_PROPERTIES);
        String project = options.getProject();

        PCollection<PubsubMessage> ibtMessages = PipelineUtil.readMessagesFromSubscription(
                pipeline,
                "Read IBT messages",
                project,
                props.get("ibtSubscriptionName").toString()
        );
        ibtMessages.apply("Process IBT Messages", new IBTTransform(options, props));

        pipeline.run();
    }
}
