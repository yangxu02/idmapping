package com.linkx.mapping.process;

import com.google.common.base.Preconditions;
import com.linkx.mapping.context.TableOutput;
import com.linkx.mapping.table.hbase.HBaseUserTable;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class UserAliasesBuilder extends Worker {
    private final static Log logger = LogFactory.getLog(UserAliasesBuilder.class);
    protected String name = UserAliasesBuilder.class.getSimpleName();

    @Override
    public Result doWork(final Row row, Result result) {
        Context context = ctx.get(row);
        if (null == context
                || null == row
                || test) return Result.ok;
        HConnection connection = ctx.gethConnection();

        tickerStart();

        boolean ok = true;
        TableOutput output = context.getOutput().getAliases();
        if (null != output) {
            String table = output.getTable();
            String cf = "";
            if (null != output.getColumnFamily()) {
                cf = output.getColumnFamily().getFamily();
            }

            Preconditions.checkNotNull(result, "user linkage must not be null");
            Preconditions.checkNotNull(result.linkage, "user linkage must not be null");

            logDuration(logger, "[task=PreconditionCheck]");

            ok = HBaseUserTable.updateAliases(result.linkage, table, cf, connection,result);
            result.setTable(table);

            logDuration(logger, "[task=updateAliases]");

            tickerReset();
        }

        result.success = ok;
        return result;
    }

}
