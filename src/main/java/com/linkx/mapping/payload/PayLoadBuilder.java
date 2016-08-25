package com.linkx.mapping.payload;

import com.linkx.mapping.process.Result;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.row.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public interface PayLoadBuilder {
    public PayLoad build(Row row, Result result, Context ctx);
}
