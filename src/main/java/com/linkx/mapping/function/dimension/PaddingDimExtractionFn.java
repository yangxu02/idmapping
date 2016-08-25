package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class PaddingDimExtractionFn extends DimensionExtractionFn {

    private String prefix = "";
    private String postfix = "";

    @JsonCreator
    public PaddingDimExtractionFn(@JsonProperty("prefix") String prefix,
                                  @JsonProperty("postfix") String postfix
    )
    {
        this.prefix = prefix;
        this.postfix = postfix;
    }

    @Override
    public String apply(String input) {
        return prefix + input + postfix;
    }

    @Override
    public String apply(Row row) {
        return "";
    }

    @Override
    public String apply(Row row, String val) {
        return apply(row.getDimensionVal(val));
    }

    @JsonProperty("prefix")
    public String getPrefix() {
        return prefix;
    }

    @JsonProperty("postfix")
    public String getPostfix() {
        return postfix;
    }

}
