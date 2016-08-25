package com.linkx.mapping.context;

import java.util.Map;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/10.
 */
public class Input {
    String format; // csv or json
    String delim = "" + '\001'; // csv delimiter
    String source; // input data source
    String type; // input data valType: event or profile

    String[] identities; // name list of identities
    String[] dimensions; // name list of all fields

    Map<String, String> refs;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFormat() {
        return format;
    }

    public String getDelim() {
        return delim;
    }

    public String getSource() {
        return source;
    }

    public String[] getIdentities() {
        return identities;
    }

    public String[] getDimensions() {
        return dimensions;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setDelim(String delim) {
        this.delim = delim;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setIdentities(String[] identities) {
        this.identities = identities;
    }

    public void setDimensions(String[] dimensions) {
        this.dimensions = dimensions;
    }

    public Map<String, String> getRefs() {
        return refs;
    }

    public void setRefs(Map<String, String> refs) {
        this.refs = refs;
    }
}
