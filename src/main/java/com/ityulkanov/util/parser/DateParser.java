package com.ityulkanov.util.parser;

import com.ityulkanov.cons.ContentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Utility class to parse date and timestamp string to BigQuery formats.
 */
public class DateParser {
    private static final Logger LOG = LoggerFactory.getLogger(DateParser.class);

    private DateParser() {
    }

    public static String convertToBQFormat(String format, String bqFormat, String value) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = parseDate(sdf, value);
        TimeZone utc = TimeZone.getTimeZone("UTC");
        SimpleDateFormat newFormat = new SimpleDateFormat(bqFormat);
        newFormat.setTimeZone(utc);
        String val = newFormat.format(date);

        if (val.startsWith(ContentInfo.SYS_GEN_DEFAULT_YEAR)) {
            val = val.replaceFirst(ContentInfo.SYS_GEN_DEFAULT_YEAR, ContentInfo.FORCED_DEFAULT_YEAR);
        }
        return val;
    }

    private static Date parseDate(SimpleDateFormat sdf, String value) throws ParseException {
        TimeZone utc = TimeZone.getTimeZone("UTC");
        sdf.setTimeZone(utc);
        sdf.setLenient(false);
        return sdf.parse(value);
    }
}
