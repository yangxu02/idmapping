package com.linkx.mapping.context;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/13.
 */
public class ProfileInfo {
    String name; // dimension name
    String col; // db column name
    String valType; // value valType


    @JsonCreator
    public ProfileInfo(@JsonProperty("name") String name,
                       @JsonProperty("col") String col,
                       @JsonProperty("valType") String valType) {
        this.name = name;
        this.col = col;
        this.valType = valType;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("col")
    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    @JsonProperty("valType")
    public String getValType() {
        return valType;
    }

    public void setValType(String valType) {
        this.valType = valType;
    }
}
