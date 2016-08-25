package com.linkx.mapping.context;

import com.linkx.mapping.function.Hasher;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/18.
 */
public class TableInfo {
    // table info
    String table;

    // hash function
    Hasher hasher = Hasher.plain;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Hasher getHasher() {
        return hasher;
    }

    public void setHasher(Hasher hasher) {
        this.hasher = hasher;
    }
}
