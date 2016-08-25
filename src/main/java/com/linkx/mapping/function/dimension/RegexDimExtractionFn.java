package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class RegexDimExtractionFn extends DimensionExtractionFn {

    private String regex;

    @JsonCreator
    public RegexDimExtractionFn(@JsonProperty("regex") String regex) {
        this.regex = regex;
    }

    @Override
    public String apply(String input) {
        if (!Strings.isNullOrEmpty(input)) {
            try {
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(input);
                if (matcher.find()) {
                    return input.substring(matcher.start(), matcher.end());
                }
            } catch (PatternSyntaxException e) {
                LogFactory.getLog(RegexDimExtractionFn.class).error("", e);
            }
        }
        return input;
    }

    @Override
    public String apply(Row row) {
        return "";
    }

    @Override
    public String apply(Row row, String dim) {
        return apply(row.getDimensionVal(dim));
    }

    @JsonProperty("regex")
    public String getRegex() {
        return regex;
    }
}
