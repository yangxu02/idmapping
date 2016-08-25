package com.linkx.mapping.process;

import com.linkx.mapping.payload.PayLoad;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.payload.EventCounterPayLoadBuilder;
import com.linkx.mapping.row.Row;
import com.linkx.mapping.table.hbase.HBaseUserTable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class UserEventCounterBuilder extends Worker {
    private final static Log logger = LogFactory.getLog(UserEventCounterBuilder.class);
    protected String name = UserEventCounterBuilder.class.getSimpleName();

    @Override
    public Result doWork(final Row row, final Result result) {
        Context context = ctx.get(row);
        if (null == context
                || null == row
                || "profile".equalsIgnoreCase(ctx.getInput().getType())
                || test) return Result.ok;
        HConnection connection = ctx.gethConnection();

        tickerStart();

        PayLoad payLoad = new EventCounterPayLoadBuilder().build(row, result, context);

        logDuration(logger, "[task=PayLoadBuild]");

        boolean success = true;
        if (null != payLoad) {
            success = HBaseUserTable.updateEventCounters(result.linkage.getUserId(), payLoad, connection,result);
        }else {
        	success = HBaseUserTable.batchUpdate(result.getPuts(), Bytes.toBytes(result.getTable()), connection);
        }

        logDuration(logger, "[task=updateEventCounters]");

        tickerReset();

        result.success = success;
        return result;
    }
}
