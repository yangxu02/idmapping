package com.linkx.mapping.table.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.inject.Inject;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.IdentityInfo;
import com.linkx.mapping.context.Mapping;
import com.linkx.mapping.context.MultipleContext;
import com.linkx.mapping.context.Output;
import com.linkx.mapping.context.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by yangxu on 12/16/14.
 */

public class HBaseTableHelper {

    private static Log logger = LogFactory.getLog(HBaseTableHelper.class);

    @Inject Context ctx;


    public boolean checkAndPrepareTables() {
        HConnection connection = ctx.gethConnection();
        Set<String> proceeded = new HashSet<>();
        if (ctx instanceof MultipleContext) {
            for (Context context : ((MultipleContext) ctx).getContexts().values()) {
                checkAndPrepareTablesV2(context, connection, proceeded);
            }
        } else {
            checkAndPrepareTablesV2(ctx, connection, proceeded);
        }
        return true;
    }

    public boolean checkAndPrepareTablesV2(Context context, HConnection connection, Set<String> proceeded) {

        Mapping mapping = context.getMapping();
        Output output = context.getOutput();
        Preconditions.checkNotNull(mapping, "mapping rules must be set before processing");
        Preconditions.checkNotNull(output, "output rules must be set before processing");

        IdentityInfo[] infos = mapping.getIdentities();
        boolean success = true;
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        long start = 0;
        long end = 0;
        Set<String> tables = new HashSet<>();
        for (IdentityInfo info : infos) {
            TableInfo[] lookUps = info.getLookup();
            if (null != lookUps && 0 != lookUps.length) {
                for (TableInfo lookUp : lookUps) {
                    tables.add(lookUp.getTable());
                }
            }
            TableInfo[] writeBacks = info.getWriteback();
            if (null != writeBacks && 0 != writeBacks.length) {
                for (TableInfo writeBack : writeBacks) {
                    tables.add(writeBack.getTable());
                }
            }
        }

        start = stopwatch.elapsedMillis();
        for (String table : tables) {
            success = HBaseMappingTables.checkAndCreateTable(table, connection);
            end = stopwatch.elapsedMillis();
            logger.info("[cost=" + (end - start) + "][task=checkAndCreateTable][table=" + table
                    + "][success=" + success + "]");
            start = end;
            if (!success) throw new RuntimeException("Create Table " + table + " failed");
        }
        proceeded.addAll(tables);
        return success;
    }


    public boolean checkAndPrepareTables(Context context, HConnection connection, Set<String> proceeded) {

        Mapping mapping = context.getMapping();
        Output output = context.getOutput();
        Preconditions.checkNotNull(mapping, "mapping rules must be set before processing");
        Preconditions.checkNotNull(output, "output rules must be set before processing");

        IdentityInfo[] infos = mapping.getIdentities();
        boolean success = true;
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        long start = 0;
        long end = 0;
        for (IdentityInfo info : infos) {
            boolean chained = info.isChained();
            TableInfo[] lookUps = info.getLookup();

            if (null != lookUps && 0 != lookUps.length) {

                if (!chained) {
                    for (TableInfo lookUp : lookUps) {
                        if (proceeded.contains(lookUp.getTable())) continue;

                        success = HBaseMappingTables.checkAndCreateTable(lookUp.getTable(), connection);
                        end = stopwatch.elapsedMillis();
                        logger.info("[cost=" + (end - start) + "][task=checkAndCreateTable][table=" + lookUp.getTable()
                                + "][type=lookup]"
                                + "][success=" + success + "]");
                        start = end;
                        proceeded.add(lookUp.getTable());
                        if (!success) return false;
                    }
                } else {

                    for (int i = 0; i < lookUps.length; ++i) {
                        TableInfo lookUp = lookUps[i];
                        if (proceeded.contains(lookUp.getTable())) continue;
                        if (i == lookUps.length - 1) {
                            success = HBaseMappingTables.checkAndCreateTable(lookUp.getTable(), connection);
                        } else {
                            success = HBaseMappingTables.isTableExists(lookUp.getTable(), connection);
                        }
                        end = stopwatch.elapsedMillis();
                        logger.info("[cost=" + (end - start) + "][task=checkAndCreateTable][table=" + lookUp.getTable()
                                + "][type=lookup-chained]"
                                + "][success=" + success + "]");
                        start = end;
                        proceeded.add(lookUp.getTable());
                        if (!success) return false;
                    }
                }
            }

            TableInfo[] writeBacks = info.getWriteback();
            if (null != writeBacks && 0 != writeBacks.length) {
                for (TableInfo writeBack : writeBacks) {
                    if (proceeded.contains(writeBack.getTable())) continue;
                    success = HBaseMappingTables.checkAndCreateTable(writeBack.getTable(), connection);
                    end = stopwatch.elapsedMillis();
                    logger.info("[cost=" + (end - start) + "][task=checkAndCreateTable][table=" + writeBack.getTable()
                            + "][type=writeback]"
                            + "][success=" + success + "]");
                    start = end;
                    proceeded.add(writeBack.getTable());
                    if (!success) return false;
                }
            }
        }
        return success;
    }
}

