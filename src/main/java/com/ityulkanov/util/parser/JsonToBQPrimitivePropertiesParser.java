package com.ityulkanov.util.parser;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.exception.InvalidDateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class JsonToBQPrimitivePropertiesParser {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToBQPrimitivePropertiesParser.class);

    private JsonToBQPrimitivePropertiesParser() {

    }

    public static String getJsonPrimitive(JsonObject columnList, String name) {
        JsonPrimitive jsonPrimitive = columnList.getAsJsonPrimitive(name);
        return jsonPrimitive != null ? jsonPrimitive.getAsString() : null;
    }

    public static BigDecimal parseNumeric(String value) {
        return new BigDecimal(value).setScale(9, RoundingMode.DOWN);
    }


    public static long parseInteger(String value) {
        return Long.parseLong(value);
    }

    public static float parseFloat(String value) {
        return Float.parseFloat(value);
    }

    public static boolean parseBoolean(String value) {
        boolean result;
        switch (value) {
            case "1":
                result = true;
                break;
            case "0":
                result = false;
                break;
            default:
                result = Boolean.parseBoolean(value);
                break;
        }
        return result;
    }

    public static String parseTimestamp(String value) {
        for (String format : supportedTimestampFormats) {
            try {
                return DateParser.convertToBQFormat(format, ContentInfo.BQ_TIMESTAMP_FORMAT, value);
            } catch (Exception ex) {
                LOG.debug("{} timestamp does not match {} format, trying next format...", value, format);
            }
        }
        throw new InvalidDateException("Timestamp is invalid");
    }

    public static String parseDate(String value) throws InvalidDateException {
        for (String format : supportedTimestampFormats) {
            try {
                return DateParser.convertToBQFormat(format, ContentInfo.BQ_DATE_FORMAT, value);
            } catch (Exception ex) {
                LOG.debug("{} date does not match {} format, trying next format...", value, format);
            }
        }
        throw new InvalidDateException("Date is invalid");
    }

    private static final String[] supportedTimestampFormats = {
            "yyyy-MM-dd'T'HH24:mm:ss.SSS'Z'",
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "yyyy-MM-dd HH:mm:ss.SSS z",
            "dd/MM/yyyy'T'HH24:mm:ss.SSS'Z'",
            "dd/MM/yyyy'T'HH:mm:ss.SSS'Z'",

            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "dd/MM/yyyy'T'HH:mm:ss.SSS",
            "dd/MM/yyyy HH:mm:ss.SSS",

            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "dd/MM/yyyy'T'HH:mm:ss'Z'",
            "yyyy-MM-dd'T'HH:mm:ssZ",
            "yyyy-MM-dd'T'HH:mm:ssXXX",

            "yyyy-MM-dd HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss",
            "dd/MM/yyyy'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss",

            "yyyy-MM-dd'T'HH:mm",

            "yyyy-MM-dd",
            "dd/MM/yyyy",
            "dd-MMM",
    };
}
