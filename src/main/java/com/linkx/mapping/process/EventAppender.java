package com.linkx.mapping.process;

import com.linkx.mapping.payload.PayLoad;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.payload.EventPayLoadBuilder;
import com.linkx.mapping.row.Row;
import com.linkx.mapping.table.hbase.HBaseEventTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class EventAppender extends Worker {
    private final static Log logger = LogFactory.getLog(EventAppender.class);
    protected String name = EventAppender.class.getSimpleName();

    @Override
    public Result doWork(final Row row, Result result) {
        Context context = ctx.get(row);
        if (null == context
                || null == row
                || test) return Result.ok;
        HConnection connection = ctx.gethConnection();
        // start ticker
        tickerStart();

        logDuration(logger, "[task=PreconditionCheck]");

        PayLoad payLoad = new EventPayLoadBuilder().build(row, result, context);

        logDuration(logger, "[task=PayLoadBuild]");

        boolean success = true;
        if (null != payLoad) {
            success = HBaseEventTable.insert(payLoad, connection);
        }

        // log work time
        logDuration(logger, "[task=PayLoadInsert]");

        // reset ticker
        tickerReset();

        result.success = success;
        return result;
    }
}
