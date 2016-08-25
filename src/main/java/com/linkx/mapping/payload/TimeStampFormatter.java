package com.linkx.mapping.payload;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.util.Date;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class TimeStampFormatter {
    static DateTimeParser[] parsers = {
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
//            DateTimeFormat.forPattern("YYYY-MM-DDThh:mm:ss").getParser(),
//            DateTimeFormat.forPattern("YYYY-MM-DDThh:mm:ssTZD").getParser(),
//            DateTimeFormat.forPattern("YYYY-MM-DDThh:mm:ss.sTZD").getParser()
    };

    static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .append( null, parsers ).toFormatter();

    static int millsLen = 13;

    public static long format(String val) {
        if (!Strings.isNullOrEmpty(val)) {
            val = val.trim();
            if (CharMatcher.DIGIT.matchesAllOf(val)) {
                return Long.parseLong(Strings.padEnd(val, millsLen, '0'));
            } else {
                try {
                    return formatter.parseDateTime(val).getMillis();
                } catch (Exception e) {
                }
            }
        }
        // use current time as default
        return new Date().getTime();
    }
}
