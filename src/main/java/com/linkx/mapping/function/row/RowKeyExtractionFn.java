package com.linkx.mapping.function.row;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "compound", value = CompoundRowKey.class),
        @JsonSubTypes.Type(name = "random", value = RandomRowKey.class),
})

public abstract class RowKeyExtractionFn {
    public abstract String apply(Row row);

    public abstract String apply(Row row, String postfix);

    public abstract String apply(String prefix, Row row);
}
