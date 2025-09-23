package com.ityulkanov.transform;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.util.ErrorTblDtl;
import com.ityulkanov.util.ExceptionUtil;
import com.ityulkanov.util.JsonBaseInfoDecorator;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * * This DoFn class will be used for convert pub/sub message to json string by unnesting it from the outer object. It
 * * will also add base info into it.
 */

@Slf4j
public class PubsubMsgToUnnestJsonDoFn extends DoFn<PubsubMessage, String> {

    private final List<String> outerObjectNames;
    private final String sourceSystemCode;
    private final String eventDateTime;
    private final String outputDirectory;

    private final TupleTag<String> recordTag;
    private final TupleTag<ErrorTblDtl> transformFail;

    public PubsubMsgToUnnestJsonDoFn(
            List<String> outerObjectNames,
            String sourceSystemCode,
            String eventDateTime,
            TupleTag<String> recordTag,
            TupleTag<ErrorTblDtl> transformFail,
            String outputDirectory) {
        this.outerObjectNames = outerObjectNames;
        this.sourceSystemCode = sourceSystemCode;
        this.eventDateTime = eventDateTime;
        this.recordTag = recordTag;
        this.transformFail = transformFail;
        this.outputDirectory = outputDirectory;
    }

    @ProcessElement
    public void processElement(ProcessContext context, MultiOutputReceiver out) {
        String messageTimestamp = context.timestamp().toString();
        String ingestionMethod = outputDirectory + ContentInfo.FILEPREFIX + messageTimestamp;
        PubsubMessage message = context.element();
        String messageStr = new String(message.getPayload(), StandardCharsets.UTF_8);
        try {
            JsonObject jsonObj = JsonParser.parseString(messageStr).getAsJsonObject();
            Gson gson = new GsonBuilder().create();
            String jsonStringWithoutNulls = gson.toJson(jsonObj);
            JsonObject cleanJson = JsonParser.parseString(jsonStringWithoutNulls).getAsJsonObject();
            log.debug("Json object parsed clean: {}", cleanJson);
            String innerJson = getInnerJsonByTag(cleanJson);
            String jsonWithBaseInfo = JsonBaseInfoDecorator.decorateJsonWithBaseInfo(
                    innerJson,
                    eventDateTime,
                    messageTimestamp,
                    sourceSystemCode,
                    ingestionMethod,
                    message.getMessageId()
            );
            out.get(recordTag).output(jsonWithBaseInfo);
        } catch (Exception e) {
            String ex = ExceptionUtil.getExceptionAsString(e);
            String timeStamp = context.timestamp().toString();
            ErrorTblDtl errTblDet = new ErrorTblDtl(sourceSystemCode, messageStr, ex, ContentInfo.ACT_CONVERT_JSON_TO_ROW, timeStamp);
            out.get(transformFail).output(errTblDet);
        }
    }

    private String getInnerJsonByTag(JsonObject cleanJson) {
        for (String outerObjectName : outerObjectNames) {
            JsonElement innerJsonObjByTag = cleanJson.get(outerObjectName);
            if (innerJsonObjByTag != null) {
                String innerJson = innerJsonObjByTag.getAsJsonObject().toString();
                log.debug("Inner json: {}", innerJson);
                return innerJson;
            }
        }
        return null;
    }
}