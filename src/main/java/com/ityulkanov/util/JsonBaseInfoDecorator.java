package com.ityulkanov.util;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ityulkanov.cons.ContentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonBaseInfoDecorator {
    private static final Logger LOG = LoggerFactory.getLogger(JsonBaseInfoDecorator.class);

    private JsonBaseInfoDecorator() {
    }

    public static String decorateJsonWithBaseInfo(
            String messageStr,
            String eventDateTime,
            String messageTimestamp,
            String sourceSystemCode,
            String ingestionMethod,
            String messageId
    ) {
        String eventdt;
        JsonObject jsonObj = JsonParser.parseString(messageStr).getAsJsonObject();
        if (jsonObj.get(eventDateTime) != null) {
            eventdt = jsonObj.getAsJsonPrimitive(eventDateTime).getAsString();
        } else {
            eventdt = messageTimestamp;
        }
        JsonObject recordJson = JsonBaseInfoDecorator.addAttributesBaseInfo(
                jsonObj, messageTimestamp, sourceSystemCode, eventdt, ingestionMethod, messageId
        );
        LOG.info("Record with base info: {}", recordJson);
        return recordJson.toString();
    }

    private static JsonObject addAttributesBaseInfo(
            JsonObject recordObj,
            String messageTimestamp,
            String sourceSystemCode,
            String eventDateTime,
            String ingestionMethod,
            String messageId) {
        JsonObject baseInfo = new JsonObject();
        baseInfo.addProperty(ContentInfo.SRC_SYSTEM_CD, sourceSystemCode);
        baseInfo.addProperty(ContentInfo.EVENT_DATE_TIME, eventDateTime);
        baseInfo.addProperty(ContentInfo.PRC_DATE_TIME, messageTimestamp);
        baseInfo.addProperty(ContentInfo.TAG_MESSAGE_ID, messageId);
        baseInfo.addProperty(ContentInfo.TAG_ING_METHOD, ingestionMethod);
        recordObj.add(ContentInfo.TAG_BASE_INFO, new Gson().toJsonTree(baseInfo));
        return recordObj;
    }
}
