package com.linkx.mapping.context;
/**
 * Created by yangxu on 1/20/15.
 */

import com.google.common.collect.ImmutableMap;
import com.linkx.mapping.filter.Expression;
import com.linkx.mapping.row.Row;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultipleContext extends Context {

    // context pool
    private ImmutableMap<Expression, Context> contexts;

    // runtime context pool
    private Map<String, Context> contextCache = new ConcurrentHashMap<>();

    // last expression used
    private Expression expr;

    public MultipleContext(ImmutableMap<Expression, Context> contexts) {
        this.contexts = contexts;
    }

    public Context get(Row row) {
        if (null == row) return null;
        // try context cache
        String cacheKey;
        Context context = null;
        if (null != expr) {
            cacheKey = expr.getCacheKey(row);
            context = contextCache.get(cacheKey);
        }
        if (null == context) { // try context pool
            for (Map.Entry<Expression, Context> entry : contexts.entrySet()) {
                if (entry.getKey().match(row)) {
                    context = entry.getValue();
                    expr = entry.getKey();
                    cacheKey = expr.getCacheKey(row);
                    contextCache.put(cacheKey, context); // caching
                    break;
                }
            }
        }

        return context;
    }

    public ImmutableMap<Expression, Context> getContexts() {
        return contexts;
    }
}
