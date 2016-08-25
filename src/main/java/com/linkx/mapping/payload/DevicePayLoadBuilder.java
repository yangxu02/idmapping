package com.linkx.mapping.payload;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.linkx.mapping.context.TableOutput;
import com.linkx.mapping.process.Result;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.DimensionInfo;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class DevicePayLoadBuilder implements PayLoadBuilder {

    private final static Log logger = LogFactory.getLog(EventPayLoadBuilder.class);
    private static final char delim = '\001';

    /**
     * build user device info payload, skip null or empty ids
     * if imei is "imei123456", data is supplied by "solo"(source), then a column will
     * be add with key="solo\001imei123456", value="imei"
     * @param row: input row contains all values and dimension names
     * @param ctx: mapping configuration context
     * @return device info to update
     */
    @Inject
    public PayLoad build(Row row, Result result, Context ctx) {

        TableOutput output = ctx.getOutput().getDevices();
        if (null == output) return null;
        // set columns
        DimensionInfo[] infos = output.getColumnFamily().getCols();
        if (null == infos || 0 == infos.length) return null;

        PayLoad payLoad = new PayLoad();
        // set table
        payLoad.setTable(output.getTable());
        // set column family
        payLoad.setColumnFamily(output.getColumnFamily().getFamily());
        // set row key
        payLoad.setRowKey(result.getLinkage().getUserId());

        for (int i = 0; i < infos.length; ++i) {
            DimensionInfo info = infos[i];
//            System.out.println(Thread.currentThread() + "@device:name=" + info.getColumn());
            // column quantifier:source\001value
            String nn = row.getDimensionVal(info.getName());
            if(Strings.isNullOrEmpty(nn)){
            	continue;
            }
            String device = info.getFn().apply(nn);
//            String device = row.getDimensionVal(info.getName());
            if (Strings.isNullOrEmpty(device)) continue;
            String key = ctx.getInput().getSource() + delim + device;
            payLoad.data.put(key, info.getColumn());
//            System.out.println(Thread.currentThread() + "@device:val=" + key);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[type=device],[payload=" + payLoad + "]");
        }

        return payLoad;
    }

}
