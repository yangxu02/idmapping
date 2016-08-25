package com.linkx.mapping.context;

import com.linkx.mapping.function.dimension.DimensionExtractionFn;
import com.linkx.mapping.function.dimension.EchoDimExtractionFn;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/10.
 */
public class DimensionInfo {
    String name;
    String column = "";
    DimensionExtractionFn fn = new EchoDimExtractionFn();
    boolean needStore = true;
    String valType = "string";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public DimensionExtractionFn getFn() {
        return fn;
    }

    public void setFn(DimensionExtractionFn fn) {
        this.fn = fn;
    }

    public boolean isNeedStore() {
        return needStore;
    }

    public void setNeedStore(boolean needStore) {
        this.needStore = needStore;
    }

    public String getValType() {
        return valType;
    }

    public void setValType(String valType) {
        this.valType = valType;
    }
}
