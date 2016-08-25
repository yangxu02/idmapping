package com.linkx.mapping.payload;

import com.google.common.base.Objects;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class PayLoad {
    String table;
    String columnFamily;
    String rowKey;
    String rowKeyPostfix = "";
    Map<String, Object> data;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public void setRowKeyPostfix(String rowKeyPostfix) {
        this.rowKeyPostfix = rowKeyPostfix;
    }

    public String getRowKeyPostfix() {
        return rowKeyPostfix;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    PayLoad() {
        data = new HashMap<>();
    }

    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("columnFamily", columnFamily)
                .add("rowKey", rowKey)
                .add("rowKeyPostfix", rowKeyPostfix)
                .add("data", new Gson().toJson(data))
                .toString();
    }

}
