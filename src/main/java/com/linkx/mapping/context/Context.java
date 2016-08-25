package com.linkx.mapping.context;

import com.linkx.mapping.row.Row;
import com.linkx.hbase.HBaseUtil;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/10.
 */
public class Context {
    Input input;
    Mapping mapping;
    Output output;
    HConnection hConnection;
    final Object lock = new Object();

    public Input getInput() {
        return input;
    }

    public void setInput(Input input) {
        this.input = input;
    }

    public Mapping getMapping() {
        return mapping;
    }

    public void setMapping(Mapping mapping) {
        this.mapping = mapping;
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    public HConnection gethConnection() {
        if (null == hConnection || hConnection.isClosed()) {
            synchronized (lock) {
                if (null == hConnection || hConnection.isClosed()) {
                    this.hConnection = HBaseUtil.getConnection();
                }
            }
        }

        return hConnection;
    }

    public void sethConnection(HConnection hConnection) {
        if (null == hConnection || hConnection.isClosed()) {
            synchronized (lock) {
                if (null == hConnection || hConnection.isClosed()) {
                    this.hConnection = HBaseUtil.getConnection();
                }
            }
        }
    }

    public Context get(Row row) {
        return this;
    }
}
