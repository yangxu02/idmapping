package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class AndDimExtractionFn extends DimensionExtractionFn {

    private DimensionExtractionFn[] fns;

    @JsonCreator
    public AndDimExtractionFn(@JsonProperty("fns") DimensionExtractionFn[] fns
    )
    {
        this.fns = fns;
    }

    @Override
    public String apply(String input) {
        String output = input;
        for (int i = 0; i < fns.length; ++i) {
            output = fns[i].apply(output);
            if (Strings.isNullOrEmpty(output)) break;
        }
        return output;
    }

    @Override
    public String apply(Row row) {
        String output = "";
        for (int i = 0; i < fns.length; ++i) {
            output = fns[i].apply(row);
        }
        return output;
    }

    @Override
    public String apply(Row row, String val) {
        String output = row.getDimensionVal(val);
        for (int i = 0; i < fns.length; ++i) {
            output = fns[i].apply(output);
        }
        return output;
    }

    @JsonProperty("fns")
    public DimensionExtractionFn[] getFns() {
        return fns;
    }
}
