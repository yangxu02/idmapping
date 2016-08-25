package com.linkx.mapping.process;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkx.mapping.table.hbase.HBaseTableHelper;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.guice.ContextModule;
import com.linkx.mapping.row.MapBasedRow;
import com.linkx.mapping.row.Row;
import com.linkx.hbase.HBaseUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 * http://ndp.confluence.dy/pages/viewpage.action?pageId=5898469
 */
public class Mapper {

    private final static Log logger = LogFactory.getLog(Mapper.class);

    // Mapping Context
    private Context ctx;

    //
    private Worker[] workers;

    private long processed = 0;

    public Mapper setup() {

        Injector injector = Guice.createInjector(new ContextModule());
        ctx = injector.getInstance(Context.class);

        workers = new Worker[] {
                injector.getInstance(UserLinkageBuilder.class), //根据配置文件的 标示 查找 userid是否存在   n个*2user表
                injector.getInstance(MappingRelationBuilder.class),// 不存在的关系补全   n个*2user表
                injector.getInstance(EventAppender.class),//   user_event
                injector.getInstance(UserProfileBuilder.class),// user_profile
                injector.getInstance(UserAliasesBuilder.class),// user_profile
                injector.getInstance(UserDevicesBuilder.class),// user_profile
                injector.getInstance(UserEventCounterBuilder.class),// user_profile
        };

        HBaseUtil.auth();

        ctx.sethConnection(HBaseUtil.getConnection());

        Preconditions.checkNotNull(ctx, "mapping context must be set");

        logger.info("[context=" + new Gson().toJson(ctx) + "][conn=" + ctx.gethConnection() + "]");

        HBaseTableHelper helper = injector.getInstance(HBaseTableHelper.class);
        Preconditions.checkState(helper.checkAndPrepareTables(), "not all tables needed setup correctly");

        return this;
    }

    public void start(String input) {
        final Row row = new MapBasedRow(input, ctx);
        if (null == row
                || null == row.getData()) {
            return;
        }

        Result result = null;
        long globalStartTime = System.currentTimeMillis();
        for (int i = 0; i < workers.length; ++i) {
        	long startTime =  System.currentTimeMillis();
            result = workers[i].doWork(row, result);
            long endTime  = System.currentTimeMillis();
            logger.info(" handler " + workers[i].getClass().getName() +"  use time " +(endTime-startTime) +".ms");
            if (!result.success) {
            	break;
            }
        }
        long globalEndTime = System.currentTimeMillis();
        logger.info(" handler one message use time " +(globalEndTime-globalStartTime) +".ms");

        ++processed;
        logger.error("-------count-------------------"+processed+"---------meesge------------");

        if ((processed & 1023) == 0) {
            logger.info("[processed=" + processed + "]");
        }
    }

    public void cleanup() {
        logger.info("[processed=" + processed + "]");
        logger.info("[conn=" + ctx.gethConnection() + "]");
        HBaseUtil.releaseConnection(ctx.gethConnection());
    }

}
