package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.linkx.mapping.row.Row;
import org.apache.commons.lang.StringUtils;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class TrimmingDimExtractionFn extends DimensionExtractionFn {

    private String str;
    private int pos = 0;

    public TrimmingDimExtractionFn(String str) {
        this.str = str;
    }


    @JsonCreator
    public TrimmingDimExtractionFn(@JsonProperty("str") String str,
                                   @JsonProperty("pos") int pos) {
        this.str = str;
        this.pos = pos;
    }

    @Override
    public String apply(String input) {
        if (!Strings.isNullOrEmpty(input)) {
            switch (pos) {
                case 0:
                    input = StringUtils.removeStart(input, str);
                    return StringUtils.removeEnd(input, str);
                case -1:
                    return StringUtils.removeStart(input, str);
                case 1:
                    return StringUtils.removeEnd(input, str);
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

    @JsonProperty("str")
    public String getStr() {
        return str;
    }

    @JsonProperty("pos")
    public int getPos() {
        return pos;
    }

}
