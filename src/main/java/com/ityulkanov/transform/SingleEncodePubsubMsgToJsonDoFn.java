package com.ityulkanov.transform;

import com.ityulkanov.util.ErrorTblDtl;
import com.ityulkanov.util.ExceptionUtil;
import com.ityulkanov.util.JsonBaseInfoDecorator;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.nio.charset.StandardCharsets;

import static com.ityulkanov.cons.ContentInfo.ACT_CONVERT_JSON_TO_ROW;
import static com.ityulkanov.cons.ContentInfo.FILEPREFIX;


/**
 * * This DoFn class will be used for convert pub/sub message to json string. It
 * * will also add base info into it.
 */
public class SingleEncodePubsubMsgToJsonDoFn extends DoFn<PubsubMessage, String> {

    private final String sourceSystemCode;
    private final String eventDateTime;
    private final String outputDirectory;
    private final TupleTag<String> recordTag;
    private final TupleTag<ErrorTblDtl> transformFail;

    public SingleEncodePubsubMsgToJsonDoFn(String sourceSystemCode, String eventDateTime,
                                           TupleTag<String> recordTag, TupleTag<ErrorTblDtl> transformFail, String outputDirectory) {
        this.sourceSystemCode = sourceSystemCode;
        this.eventDateTime = eventDateTime;
        this.recordTag = recordTag;
        this.transformFail = transformFail;
        this.outputDirectory = outputDirectory;
    }

    @ProcessElement
    public void processElement(ProcessContext context, MultiOutputReceiver out) {
        String msgTmstmp = context.timestamp().toString();
        String ingasMethod = outputDirectory + FILEPREFIX + msgTmstmp;

        PubsubMessage message = context.element();
        String messageStr;
        messageStr = new String(message.getPayload(), StandardCharsets.UTF_8);

        try {
            String baseInfo = JsonBaseInfoDecorator.decorateJsonWithBaseInfo(
                    messageStr,
                    eventDateTime,
                    msgTmstmp,
                    sourceSystemCode,
                    ingasMethod,
                    message.getMessageId()
            );

            out.get(recordTag).output(baseInfo);
        } catch (Exception e) {
            String excepAsString = ExceptionUtil.getExceptionAsString(e);
            String timeStamp = context.timestamp().toString();

            ErrorTblDtl errTblDet = new ErrorTblDtl(sourceSystemCode, messageStr, excepAsString,
                    ACT_CONVERT_JSON_TO_ROW, timeStamp);
            out.get(transformFail).output(errTblDet);

        }
    }
}