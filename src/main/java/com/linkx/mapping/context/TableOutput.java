package com.linkx.mapping.context;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/18.
 */
public class TableOutput {
    String table;

    RowKeyInfo rowKey;

    ColumnFamilyInfo columnFamily;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public RowKeyInfo getRowKey() {
        return rowKey;
    }

    public void setRowKey(RowKeyInfo rowKey) {
        this.rowKey = rowKey;
    }

    public ColumnFamilyInfo getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(ColumnFamilyInfo columnFamily) {
        this.columnFamily = columnFamily;
    }
}
