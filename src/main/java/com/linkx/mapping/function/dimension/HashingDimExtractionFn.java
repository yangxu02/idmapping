package com.linkx.mapping.function.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.linkx.mapping.function.Hasher;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class HashingDimExtractionFn extends DimensionExtractionFn {

    private String prefix = "";
    private String postfix = "";
    private Hasher hasher;

    @JsonCreator
    public HashingDimExtractionFn(@JsonProperty("hasher") Hasher hasher,
                                  @JsonProperty("prefix") String prefix,
                                  @JsonProperty("postfix") String postfix
                                  )
    {
        this.hasher = hasher;
        this.prefix = Strings.nullToEmpty(prefix);
        this.postfix = Strings.nullToEmpty(postfix);
    }

    @Override
    public String apply(String input) {
        return hasher.hash(prefix + input + postfix);
    }

    @Override
    public String apply(Row row) {
        return "";
    }

    @Override
    public String apply(Row row, String val) {
        return apply(row.getDimensionVal(val));
    }

    @JsonProperty("hasher")
    public Hasher getHasher() {
        return hasher;
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
