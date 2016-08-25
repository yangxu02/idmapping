package com.linkx.mapping.context;

import com.linkx.mapping.function.dimension.DimensionExtractionFn;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class CounterInfo {
    String dim; // dime name, used when an add needed
    String event; // event name
    String name; // counter name
    DimensionExtractionFn[] fns;

    public String getDim() {
        return dim;
    }

    public void setDim(String dim) {
        this.dim = dim;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DimensionExtractionFn[] getFns() {
        return fns;
    }

    public void setFns(DimensionExtractionFn[] fns) {
        this.fns = fns;
    }
}
