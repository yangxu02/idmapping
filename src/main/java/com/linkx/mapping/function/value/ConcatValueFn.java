package com.linkx.mapping.function.value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import java.util.Collection;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/18.
 */
public class ConcatValueFn implements ValueJoinFn {

    private String connector = "" + '\001';

    @JsonCreator
    public ConcatValueFn(@JsonProperty("connector") String connector) {
        this.connector = connector;
    }

    @JsonCreator
    public ConcatValueFn() {
    }


    @Override
    public String apply(String... values) {
        if (null != values && 0 != values.length) {
            return Joiner.on(connector).useForNull("").join(values);
        }
        return "";
    }

    @Override
    public String apply(Collection<String> values) {
        if (null != values && !values.isEmpty()) {
            return Joiner.on(connector).useForNull("").join(values);
        }
        return "";
    }

    @JsonProperty("connector")
    public String getConnector() {
        return connector;
    }
}
