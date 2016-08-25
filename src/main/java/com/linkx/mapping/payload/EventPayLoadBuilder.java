package com.linkx.mapping.payload;

import com.google.gson.Gson;
import com.linkx.mapping.context.TableOutput;
import com.linkx.mapping.process.Result;
import com.linkx.mapping.row.Row;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.DimensionInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class EventPayLoadBuilder implements PayLoadBuilder {

    private final static Log logger = LogFactory.getLog(EventPayLoadBuilder.class);

    public PayLoad build(Row row, Result result, Context ctx) {

        TableOutput output = ctx.getOutput().getEvent();
        if (null == output) return null;

        DimensionInfo[] infos = output.getColumnFamily().getCols();
        if (null == infos || 0 == infos.length) return null;

        PayLoad payLoad = new PayLoad();
        // set table
        payLoad.setTable(output.getTable());
        // set column family
        payLoad.setColumnFamily(output.getColumnFamily().getFamily());

        // set primary row key
        payLoad.setRowKey(result.getLinkage().getUserId());

        // set row key postfix
        payLoad.setRowKeyPostfix(output.getRowKey().getFn().apply(row));
        for (int i = 0; i < infos.length; ++i) {
            DimensionInfo info = infos[i];
            if (info.isNeedStore()) {
                payLoad.data.put(info.getColumn(),
                        info.getFn().apply(row, info.getName())
                );
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[type=event],[payload=" + new Gson().toJson(payLoad.getData()) + "]");
        }
        return payLoad;
    }

}
