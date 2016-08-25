package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class CsvPackingDimExtractionFn extends DimensionExtractionFn {

    private char separator = '\001';

    private Joiner joiner;

    private String[] dims = null;

    public CsvPackingDimExtractionFn() {
        joiner = Joiner.on(separator);
    }

    public CsvPackingDimExtractionFn(char separator) {
        this.separator = separator;
        joiner = Joiner.on(separator);
    }

    public CsvPackingDimExtractionFn(String[] dims) {
        this.dims = dims;
        joiner = Joiner.on(separator);
    }


    @JsonCreator
    public CsvPackingDimExtractionFn(@JsonProperty("separator") char separator,
                                     @JsonProperty("dims") String[] dims) {
        this.separator = separator;
        this.dims = dims;
        joiner = Joiner.on(separator);
    }

    @JsonProperty("separator")
    public char getSeparator() {
        return separator;
    }

    @JsonProperty("dims")
    public String[] getDims() {
        return dims;
    }

    @Override
    public String apply(String input) {
        return "";
    }

    @Override
    public String apply(Row row) {
        return joiner.join(row.getValues(dims));
    }

    @Override
    public String apply(Row row, String dim) {
        return apply(row);
    }
}
