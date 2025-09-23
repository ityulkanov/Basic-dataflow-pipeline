package com.ityulkanov.transform;

import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.dataflow.options.CombinedJobCommonOptions;
import com.ityulkanov.util.AvroPubsubMessageRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.NonNull;
import teleport.io.WindowedFilenamePolicy;
import teleport.util.DurationUtils;

/**
 * This PTransform class will be used for writing pub/sub messages to AVRO files
 * in GCS
 */

@Slf4j
public class AvroTransformV2 extends PTransform<PCollection<PubsubMessage>, PDone> {
    String outputDirectory;
    transient CombinedJobCommonOptions options;

    public AvroTransformV2(
            String outputDirectory,
            CombinedJobCommonOptions options
    ) {
        this.outputDirectory = outputDirectory;
        this.options = options;
    }

    @Override
    @NonNull
    public PDone expand(PCollection<PubsubMessage> msgCollection) {
        return msgCollection.apply(ContentInfo.INFO_MAP_TO_ARCHIVE, ParDo.of(new PubsubMessageToArchiveDoFn()))
                .apply(options.getWindowDuration() + ContentInfo.INFO_WINDOW,
                        Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
                .apply(ContentInfo.INFO_WRITE_AVRO_FILES,
                        AvroIO.write(AvroPubsubMessageRecord.class)
                                .to(new WindowedFilenamePolicy(outputDirectory,
                                        options.getOutputFilenamePrefix().get(),
                                        "W-P-SS-of-NN", options.getOutputFilenameSuffix().get()))
                                .withTempDirectory(NestedValueProvider.of(options.getAvroTempDirectory(),
                                        (SerializableFunction<String, ResourceId>) FileBasedSink::convertToFileResourceIfPossible))
                                .withWindowedWrites().withNumShards(options.getNumShards()));
    }

    static class PubsubMessageToArchiveDoFn extends DoFn<PubsubMessage, AvroPubsubMessageRecord> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            context.output(new AvroPubsubMessageRecord(message.getPayload(), message.getAttributeMap(),
                    context.timestamp().getMillis()));
        }
    }
}
