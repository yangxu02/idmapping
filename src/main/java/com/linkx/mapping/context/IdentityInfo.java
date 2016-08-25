package com.linkx.mapping.context;

import com.linkx.mapping.function.value.ConcatValueFn;
import com.linkx.mapping.function.value.ValueJoinFn;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/10.
 */
public class IdentityInfo {
    // identity dimension name
    String name;

    // tables to lookup
    String[] path;

    // dimension to generate identity
    DimensionInfo[] dims;

    // join dimension values
    ValueJoinFn joiner = new ConcatValueFn();

    // tables to lookup
    TableInfo[] lookup;

    /**
     * find user by table chain or just probing
     */
    boolean chained = false;

    /**
     * if identity2user not found, skip this record
     */
    boolean skipNulls = false;

    // tables to write back
    TableInfo[] writeback;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getPath() {
        return path;
    }

    public void setPath(String[] path) {
        this.path = path;
    }

    public boolean isChained() {
        return chained;
    }

    public void setChained(boolean chained) {
        this.chained = chained;
    }

    public DimensionInfo[] getDims() {
        return dims;
    }

    public void setDims(DimensionInfo[] dims) {
        this.dims = dims;
    }

    public TableInfo[] getLookup() {
        return lookup;
    }

    public void setLookup(TableInfo[] lookup) {
        this.lookup = lookup;
    }

    public TableInfo[] getWriteback() {
        return writeback;
    }

    public void setWriteback(TableInfo[] writeback) {
        this.writeback = writeback;
    }

    public ValueJoinFn getJoiner() {
        return joiner;
    }

    public boolean isSkipNulls() {
        return skipNulls;
    }

    public void setSkipNulls(boolean skipNulls) {
        this.skipNulls = skipNulls;
    }
}
