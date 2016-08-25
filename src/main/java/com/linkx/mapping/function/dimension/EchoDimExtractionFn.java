package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class EchoDimExtractionFn extends DimensionExtractionFn {

    @JsonCreator
    public EchoDimExtractionFn() {
    }

    @Override
    public String apply(String input) {
        return input;
    }

    @Override
    public String apply(Row row) {
        return "";
    }

    @Override
    public String apply(Row row, String dim) {
        return row.getDimensionVal(dim);
    }
}
