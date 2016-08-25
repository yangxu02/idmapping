package com.linkx.mapping.function.value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkx.mapping.function.Hasher;

import java.util.Collection;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/18.
 */
public class HashingValueFn implements ValueJoinFn {

    private Hasher hasher;

    @JsonCreator
    public HashingValueFn(@JsonProperty("hasher") Hasher hasher) {
        this.hasher = hasher;
    }

    @Override
    public String apply(String... values) {
        if (null != values && 0 != values.length) {
            return hasher.hash(values[0]);
        }
        return "";
    }

    @Override
    public String apply(Collection<String> values) {
        if (null != values && !values.isEmpty()) {
            return hasher.hash(values.iterator().next());
        }
        return "";
    }

    @JsonProperty("hasher")
    public Hasher getHasher() {
        return hasher;
    }
}
