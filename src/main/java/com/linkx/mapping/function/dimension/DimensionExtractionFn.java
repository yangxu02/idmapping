package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/10.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "echo", value = EchoDimExtractionFn.class),
        @JsonSubTypes.Type(name = "csvpack", value = CsvPackingDimExtractionFn.class),
        @JsonSubTypes.Type(name = "jsonpack", value = JsonPackingDimExtractionFn.class),
        @JsonSubTypes.Type(name = "jsonmap", value = JsonMapDimExtractionFn.class),
        @JsonSubTypes.Type(name = "scale", value = ScalingDimExtractionFn.class),
        @JsonSubTypes.Type(name = "const", value = ConstantDimExtractionFn.class),
        @JsonSubTypes.Type(name = "and", value = AndDimExtractionFn.class),
        @JsonSubTypes.Type(name = "hash", value = HashingDimExtractionFn.class),
        @JsonSubTypes.Type(name = "pad", value = PaddingDimExtractionFn.class),
        @JsonSubTypes.Type(name = "trim", value = TrimmingDimExtractionFn.class),
        @JsonSubTypes.Type(name = "regex", value = RegexDimExtractionFn.class),
})

public abstract class DimensionExtractionFn {

    public abstract String apply(String input);

    public abstract String apply(Row row);

    public abstract String apply(Row row, String dim);

}
