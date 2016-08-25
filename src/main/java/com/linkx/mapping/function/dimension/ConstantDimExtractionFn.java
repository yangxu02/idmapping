package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class ConstantDimExtractionFn extends DimensionExtractionFn {

    private String value;

    @JsonCreator
    public ConstantDimExtractionFn(@JsonProperty("value") String value) {
        this.value = value;
    }

    @Override
    public String apply(String input) {
        return value;
    }

    @Override
    public String apply(Row row) {
        return value;
    }

    @Override
    public String apply(Row row, String val) {
        return value;
    }

    @JsonProperty("value")
    public String getValue() {
        return value;
    }
}
