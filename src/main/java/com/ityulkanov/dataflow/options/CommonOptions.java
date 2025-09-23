package com.ityulkanov.dataflow.options;

import com.ityulkanov.cons.ContentInfo;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface CommonOptions extends PipelineOptions, StreamingOptions, DataflowPipelineOptions {

	@Description(ContentInfo.DESCRIPTION_READ_FROM_PUBSUB_TOPIC)
    ValueProvider<String> getInputTopic();

	void setInputTopic(ValueProvider<String> value);

    @Description(ContentInfo.DESCRIPTION_READ_FROM_PUBSUB_SUBSCRIPTION)
	ValueProvider<String> getSubscription();

	void setSubscription(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_DIRECTORY_TO_OUTPUT_FILES)
	@Validation.Required
	ValueProvider<String> getOutputDirectory();

	void setOutputDirectory(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_PREFIX_OF_FILES_TO_WRITE_TO)
	@Default.String("output")
	ValueProvider<String> getOutputFilenamePrefix();

	void setOutputFilenamePrefix(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_SUFFIX_OF_FILES_TO_WRITE_TO)
	@Default.String("")
	ValueProvider<String> getOutputFilenameSuffix();

	void setOutputFilenameSuffix(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_OUTPUT_SHARD_TEMPLATE)

	@Default.String("W-P-SS-of-NN")
	ValueProvider<String> getOutputShardTemplate();

	void setOutputShardTemplate(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_MAX_NUM_OUTPUT_SHARDS)
	@Default.Integer(1)
	Integer getNumShards();

	void setNumShards(Integer value);

	@Description(ContentInfo.DESCRIPTION_OUTPUT_SHARD_TEMPLATE)

	@Default.String(ContentInfo.DESCRIPTION_WINDOW_DURATION)
	String getWindowDuration();

	void setWindowDuration(String value);

	@Description(ContentInfo.DESCRIPTION_AVRO_WRITE_TEMP_DIRECTORY)
	@Validation.Required
	ValueProvider<String> getAvroTempDirectory();

	void setAvroTempDirectory(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_DATASET_TO_WRITE_CL_OUTPOUT_TO)
	@Validation.Required
	ValueProvider<String> getDataSet();

	void setDataSet(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_TBL_SPEC_TO_WRITE_CL_ERR_OUTPUT)
	ValueProvider<String> getErrTableSpec();

	void setErrTableSpec(ValueProvider<String> value);

        @Description(ContentInfo.DESCRIPTION_DATA_SET_TRUSTED)
	@Validation.Required

	ValueProvider<String> getTrustDataSet();

	void setTrustDataSet(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_TRUSTED_ERR_TBL)
	@Validation.Required

	ValueProvider<String> getTrustErrTbl();

	void setTrustErrTbl(ValueProvider<String> value);

	// Specific to Tactical Dataset
	@Description(ContentInfo.DESCRIPTION_DATASET_TACTICAL)
	@Validation.Required

	ValueProvider<String> getTacticalDataSet();

	void setTacticalDataSet(ValueProvider<String> value);

	@Description(ContentInfo.DESCRIPTION_TACTICAL_ERR_TBL)
	@Validation.Required

	ValueProvider<String> getTacticalErrTbl();

	void setTacticalErrTbl(ValueProvider<String> value);


}