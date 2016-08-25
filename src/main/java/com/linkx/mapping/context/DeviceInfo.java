package com.linkx.mapping.context;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class DeviceInfo {
    String name;
    String valType;

    @JsonCreator
    public DeviceInfo(@JsonProperty("name") String name,
                      @JsonProperty("valType") String valType) {
        this.name = name;
        this.valType = valType;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("valType")
    public String getValType() {
        return valType;
    }

    public void setValType(String valType) {
        this.valType = valType;
    }
}
