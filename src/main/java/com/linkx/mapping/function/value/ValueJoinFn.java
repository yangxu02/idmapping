package com.linkx.mapping.function.value;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Collection;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/18.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "concat", value = ConcatValueFn.class),
        @JsonSubTypes.Type(name = "hash", value = HashingValueFn.class)
})

public interface ValueJoinFn {
    public String apply(String... values);

    public String apply(Collection<String> values);
}
