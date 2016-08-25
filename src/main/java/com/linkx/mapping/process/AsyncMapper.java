package com.linkx.mapping.process;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.*;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.guice.ContextModule;
import com.linkx.mapping.row.MapBasedRow;
import com.linkx.mapping.row.Row;
import com.linkx.mapping.table.hbase.HBaseTableHelper;
import com.linkx.hbase.HBaseUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class AsyncMapper {

    private final static Log logger = LogFactory.getLog(AsyncMapper.class);

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    Context ctx;

    private Worker[] workers;

    private long processed = 0;

    public AsyncMapper setup() {

        Injector injector = Guice.createInjector(new ContextModule());
        ctx = injector.getInstance(Context.class);

        workers = new Worker[] {
                injector.getInstance(UserLinkageBuilder.class),
                injector.getInstance(MappingRelationBuilder.class),
                injector.getInstance(EventAppender.class),
                injector.getInstance(UserProfileBuilder.class),
                injector.getInstance(UserAliasesBuilder.class),
                injector.getInstance(UserDevicesBuilder.class),
                injector.getInstance(UserEventCounterBuilder.class),
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
        // construct user linkage
        ListenableFuture<Result> result = executor.submit(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                return workers[0].doWork(row, null);
            }
        });

        for (int i = 1; i < workers.length; ++i) {
            result = Futures.transform(result, functions(workers[i], row), executor);
            try {
                if (!result.get().success) {
                    break;
                }
            } catch (InterruptedException e) {
                logger.error("", e);
            } catch (ExecutionException e) {
                logger.error("", e);
            }
        }

        ++processed;

        if ((processed & 1023) == 0) {
            logger.info("[processed=" + processed + "]");
        }

    }

    // update mapping
    AsyncFunction<Result, Result> functions(final Worker worker, final Row row) {
        return new AsyncFunction<Result, Result>() {
            @Override
            public ListenableFuture<Result> apply(Result result) throws Exception {
                return Futures.immediateFuture(worker.doWork(row, result));
            }
        };
    }

    public void cleanup() {
        logger.info("[conn=" + ctx.gethConnection() + "]");
        HBaseUtil.releaseConnection(ctx.gethConnection());
    }

}
