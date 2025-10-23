package com.ityulkanov.transform;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ityulkanov.util.ErrorTblDtl;
import com.ityulkanov.util.ExceptionUtil;
import com.ityulkanov.util.JsonOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Properties;

import static com.ityulkanov.cons.ContentInfo.EVENT_DATE_TIME;
import static com.ityulkanov.cons.ContentInfo.TAG_BASE_INFO;
import static com.ityulkanov.cons.ContentInfo.TBL_FIELD_SRC_SYS_NAME;
import static com.ityulkanov.cons.IBTConstants.ACTOR;
import static com.ityulkanov.cons.IBTConstants.ACTOR_TYPE;
import static com.ityulkanov.cons.IBTConstants.CODE;
import static com.ityulkanov.cons.IBTConstants.DESCRIPTION;
import static com.ityulkanov.cons.IBTConstants.DEVICE_TYPE;
import static com.ityulkanov.cons.IBTConstants.EVENT_CATEGORY;
import static com.ityulkanov.cons.IBTConstants.EVENT_METADATA;
import static com.ityulkanov.cons.IBTConstants.EVENT_SOURCE;
import static com.ityulkanov.cons.IBTConstants.EVENT_TIMESTAMP;
import static com.ityulkanov.cons.IBTConstants.EVENT_TYPE;
import static com.ityulkanov.cons.IBTConstants.PAYLOAD;
import static com.ityulkanov.cons.IBTConstants.REASON;
import static com.ityulkanov.cons.IBTConstants.REASON_CODE;
import static com.ityulkanov.cons.IBTConstants.REASON_DESCRIPTION;
import static com.ityulkanov.cons.IBTConstants.SYSTEM_NAME;
import static com.ityulkanov.cons.IBTConstants.TYPE;
import static com.ityulkanov.cons.IBTConstants.USER_ID;
import static com.ityulkanov.util.JsonOperation.copyProperty;

@Slf4j
public class IbtTrustedFn extends DoFn<String, String> {

    private final TupleTag<String> recordTag;
    private final TupleTag<ErrorTblDtl> transformFail;

    public IbtTrustedFn(TupleTag<String> recordTag, TupleTag<ErrorTblDtl> transformFail) {
        this.recordTag = recordTag;
        this.transformFail = transformFail;
    }

    @ProcessElement
    public void processElement(ProcessContext c, MultiOutputReceiver out) {
        String jsonString = c.element();
        try {
            log.debug("Process element in IbtTrustedDoFn. Object received : {}", jsonString);
            if (jsonString == null) {
                throw new IllegalArgumentException("Input JSON string is null");
            }
            JsonObject root = JsonParser.parseString(jsonString).getAsJsonObject();

            // extract nested objects
            JsonObject eventMetadata = root.getAsJsonObject(EVENT_METADATA);
            JsonObject baseInfo = JsonOperation.safeJsonObject(root, TAG_BASE_INFO);
            JsonObject actor = JsonOperation.safeJsonObject(eventMetadata, ACTOR);
            JsonObject payload = JsonOperation.safeJsonObject(root, PAYLOAD);
            JsonObject reason = JsonOperation.safeJsonObject(payload, REASON);

            // move all payload fields to root
            JsonOperation.moveAllFieldsToRoot(root, PAYLOAD);

            // set up base info fields
            copyProperty(eventMetadata, EVENT_TYPE, baseInfo, EVENT_TYPE);
            copyProperty(eventMetadata, EVENT_SOURCE, baseInfo, TBL_FIELD_SRC_SYS_NAME);
            copyProperty(eventMetadata, EVENT_TIMESTAMP, baseInfo, EVENT_DATE_TIME);
            copyProperty(eventMetadata, EVENT_CATEGORY, baseInfo, EVENT_CATEGORY);

            // set up reason and actor fields
            copyProperty(reason, CODE, root, REASON_CODE);
            copyProperty(reason, DESCRIPTION, root, REASON_DESCRIPTION);
            copyProperty(actor, TYPE, root, ACTOR_TYPE);
            copyProperty(actor, SYSTEM_NAME, root, SYSTEM_NAME);
            copyProperty(actor, USER_ID, root, USER_ID);
            copyProperty(actor, DEVICE_TYPE, root, DEVICE_TYPE);
            // remove eventMetadata from root
            root.remove(EVENT_METADATA);
            out.get(recordTag).output(root.toString());
        } catch (Exception e) {
            String excepAsString = ExceptionUtil.getExceptionAsString(e);
            String timeStamp = c.timestamp().toString();
            ErrorTblDtl errTblDet = new ErrorTblDtl("IBT", jsonString, excepAsString,
                    "IBT Transformation failed", timeStamp);
            out.get(transformFail).output(errTblDet);
        }
    }
}
