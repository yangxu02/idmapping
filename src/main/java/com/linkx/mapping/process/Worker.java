package com.linkx.mapping.process;

import com.google.common.base.Stopwatch;
import com.google.inject.Inject;
import com.linkx.mapping.row.Row;
import com.linkx.mapping.context.Context;
import org.apache.commons.logging.Log;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public abstract class Worker {

    @Inject
    protected Context ctx;

    public abstract Result doWork(final Row row, Result result);

    protected final boolean test = false;

    protected final Stopwatch ticker = new Stopwatch();
    protected long lastElapsed = 0;
    protected String name = "BaseWorker";

    protected void tickerStart() {
        if (!ticker.isRunning()) ticker.start();
        lastElapsed = ticker.elapsedMillis();
    }

    protected void tickerReset() {
        ticker.reset();
    }

    protected void tickerStop() {
        if (ticker.isRunning()) ticker.stop();
    }

    protected void logDuration(Log log, String msg) {
        long elapsed = ticker.elapsedMillis();
        log.info( "[cost=" + (elapsed - lastElapsed) + "]" + msg );
        lastElapsed = elapsed;
    }


}
