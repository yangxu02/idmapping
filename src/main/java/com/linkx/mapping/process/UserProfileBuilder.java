package com.linkx.mapping.process;

import com.google.common.base.Preconditions;
import com.linkx.mapping.payload.PayLoad;
import com.linkx.mapping.table.hbase.HBaseUserTable;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.payload.ProfilePayLoadBuilder;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class UserProfileBuilder extends Worker {
    private final static Log logger = LogFactory.getLog(UserProfileBuilder.class);
    protected String name = UserProfileBuilder.class.getSimpleName();

    @Override
    public Result doWork(final Row row, Result result) {
        Context context = ctx.get(row);
        if (null == context
                || null == row
                || test) return Result.ok;
        HConnection connection = ctx.gethConnection();

        tickerStart();

        Preconditions.checkNotNull(result, "user linkage must not be null");
        Preconditions.checkNotNull(result.linkage, "user linkage must not be null");

        logDuration(logger, "[task=PreconditionCheck]");

        PayLoad payLoad = new ProfilePayLoadBuilder().build(row, result, context);

        logDuration(logger, "[task=PayLoadBuild]");

        boolean success = true;

        if (null != payLoad) {
        	//do not handler hbase  just put put to result
            HBaseUserTable.updateProfile(payLoad.getRowKey(), payLoad, connection,result);
            result.setTable(payLoad.getTable());
        }

        logDuration(logger, "[task=updateProfile]");

        tickerReset();

        result.success = success;
        return result;
    }

}
