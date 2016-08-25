package com.linkx.mapping.context;

import java.util.Set;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class EventCounters {
    String table; // table to store counters
    String columnFamily; // column family to store counters
    String timeField; // timestamp fields
    String eventField; // event valType field
    Set<String> skips; // skipped event valType
    CounterInfo[] extras; // extra counters

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }

    public String getEventField() {
        return eventField;
    }

    public void setEventField(String eventField) {
        this.eventField = eventField;
    }

    public Set<String> getSkips() {
        return skips;
    }

    public void setSkips(Set<String> skips) {
        this.skips = skips;
    }

    public CounterInfo[] getExtras() {
        return extras;
    }

    public void setExtras(CounterInfo[] extras) {
        this.extras = extras;
    }
}
