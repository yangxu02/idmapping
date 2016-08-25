package com.linkx.mapping.process;

import com.google.common.base.Preconditions;
import com.linkx.mapping.payload.PayLoad;
import com.linkx.mapping.table.hbase.HBaseUserTable;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.payload.DevicePayLoadBuilder;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class UserDevicesBuilder extends Worker {
    private final static Log logger = LogFactory.getLog(UserDevicesBuilder.class);
    protected String name = UserDevicesBuilder.class.getSimpleName();

    @Override
    public Result doWork(final Row row, Result result) {
        Context context = ctx.get(row);
        if (null == context
                || null == row
                || test) return Result.ok;
        HConnection connection = ctx.gethConnection();

        tickerStart();

        Preconditions.checkNotNull(context.getMapping(), "mapping context must be set before processing");
        Preconditions.checkNotNull(result, "inter media result must be set before processing");
        Preconditions.checkNotNull(result.linkage, "inter media result must be set before processing");

        logDuration(logger, "[task=PreconditionCheck]");

        PayLoad payLoad = new DevicePayLoadBuilder().build(row, result, context);

        logDuration(logger, "[task=PayLoadBuild]");

        boolean success = true;
        if (null != payLoad) {
            success = HBaseUserTable.updateDevices(payLoad.getRowKey(), payLoad, connection,result);
            result.setTable(payLoad.getTable());
        }

        logDuration(logger, "[task=UpdateDevices]");

        tickerReset();

        result.success = success;
        return result;
    }
}
