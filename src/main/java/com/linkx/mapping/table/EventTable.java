package com.linkx.mapping.table;

import com.linkx.mapping.payload.PayLoad;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * Created by yangxu on 12/16/14.
 */
public interface EventTable {

    public boolean insert(String key, PayLoad payLoad, HConnection connection);

    public boolean insert(PayLoad payLoad, HConnection connection);

}
