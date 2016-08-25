package com.linkx.mapping.process;

import com.linkx.mapping.row.Row;
import com.linkx.mapping.table.hbase.HBaseMappingTables;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.linkx.mapping.context.Context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 * build identity -> user mapping relation
 */
public class MappingRelationBuilder extends Worker {

    private final static Log logger = LogFactory.getLog(MappingRelationBuilder.class);
    protected String name = MappingRelationBuilder.class.getSimpleName();

    @Override
    public Result doWork(final Row row, Result result) {
        Context context = ctx.get(row);
        if (null == context
                || null == row
                || test) return Result.ok;
        HConnection connection = ctx.gethConnection();

        tickerStart();

        Preconditions.checkNotNull(result, "result must be set before processing");
        Preconditions.checkNotNull(result.linkage, "result must be set before processing");
        UserLinkage linkage = result.linkage;
        Preconditions.checkNotNull(linkage, "user linkage context must be set before processing");

        logDuration(logger, "[task=PreconditionCheck]");

        if (logger.isDebugEnabled()) {
            logger.debug(new Gson().toJson(linkage));
        }

        String userId = linkage.getUserId();
        List<Put> puts = new ArrayList<Put>();
        Put put = null;
        String table = "";
        for (Linkage link : linkage.getLinkages()) {
            if (userId.equalsIgnoreCase(link.getUserId())) continue; // user id not change, skip
            table = link.getTable();
            //put = new Put(HBaseMappingTables.rowKeyGenerator.from(link.getIdentity(),HBaseMappingTables.nRegions));
            put = new Put(Bytes.toBytes(link.getIdentity()+"|"+link.getOldTbaleName()));
            put.add(HBaseMappingTables.Family, HBaseMappingTables.Quantifier, Bytes.toBytes(userId));
            puts.add(put);
            
            //HBaseMappingTables.update(link.getIdentity(), link.getTable(), userId, connection);

           /* logDuration(logger, "[task=UpdateLinkage"
                    + "][link.id=" + link.getIdentity()
                    + "][link.table=" + link.getTable()
                    + "][link.user=" + link.getUserId()
                    + "][user=" + userId
                    + "]"
            );*/
        }
        if(!"".equals(table)){
        	HBaseMappingTables.batchUpdate(puts, table, connection);
        }

        tickerReset();

        result.success = true;
        return result;
    }
}
