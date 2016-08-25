package com.linkx.mapping.payload;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.linkx.mapping.process.Result;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.CounterInfo;
import com.linkx.mapping.context.EventCounters;
import com.linkx.mapping.function.dimension.DimensionExtractionFn;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class EventCounterPayLoadBuilder implements PayLoadBuilder {

    private final static Log logger = LogFactory.getLog(EventPayLoadBuilder.class);

    @Override
    public PayLoad build(Row row, Result result, Context ctx) {
        EventCounters counters = ctx.getOutput().getCounters();
        if (null == counters) return null;
        String event = row.getDimensionVal(counters.getEventField());
        if (Strings.isNullOrEmpty(event)) return null;
        if (counters.getSkips().contains(event)) { // no need for this event type
            return null;
        }

        PayLoad payLoad = new PayLoad();
        // set table
        payLoad.setTable(counters.getTable());
        // set column family
        payLoad.setColumnFamily(counters.getColumnFamily());
        // set row key
        payLoad.setRowKey(result.getLinkage().getUserId());

        // add timestamp
        long timestamp = TimeStampFormatter.format(row.getDimensionVal(counters.getTimeField()));
        payLoad.data.put(event + "@last", timestamp);

        // add sum counter
        payLoad.data.put(event + "@sum", 1L);

        if (null != counters.getExtras()) {
            // add extra counter
            for (CounterInfo info : counters.getExtras()) {
                if (event.equalsIgnoreCase(info.getEvent())) {
                    String strVal = row.getDimensionVal(info.getDim());
                    DimensionExtractionFn[] fns = info.getFns();
                    for (int i = 0; i < fns.length; ++i) {
                        strVal = fns[i].apply(strVal);
                    }
                    long val = 0;
                    try {
                        val = Long.parseLong(strVal);
                    } catch (Exception e) {

                    }
                    if (val > 0) {
                        payLoad.data.put(event + "@" + info.getName(), val);
                    }
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[type=eventcounter],[payload=" + new Gson().toJson(payLoad.getData()) + "]");
        }

        return payLoad;
    }
}
