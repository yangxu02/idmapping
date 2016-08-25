package com.linkx.mapping.filter;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.linkx.mapping.function.dimension.EchoDimExtractionFn;
import com.linkx.mapping.function.dimension.DimensionExtractionFn;
import com.linkx.mapping.row.Row;

import java.util.Set;

/**
 * Created by yangxu on 1/20/15.
 */

public class SimpleExpression implements Expression {

    String expr;
    String key;
    OP op;
    String val;
    Set<String> vals;
    DimensionExtractionFn fn = new EchoDimExtractionFn();

    SimpleExpression(String expr) {
        this.expr = expr;
        this.compile();
    }

    public DimensionExtractionFn getFn() {
        return fn;
    }

    public void setFn(DimensionExtractionFn fn) {
        this.fn = fn;
    }

    private void compile() {
        int s = 0;
        int e = expr.indexOf(' ', s);
        Preconditions.checkArgument(-1 != e, "invalid expression");
        key = expr.substring(s, e);
        while (' ' == expr.charAt(e++)); // skip spaces
        s = e - 1;
        e = expr.indexOf(' ', s);
        Preconditions.checkArgument(-1 != e, "invalid expression");
        op = OP.valueOf(expr.substring(s, e).toLowerCase());
        while (' ' == expr.charAt(e++)); // skip spaces
        s = e - 1;
        switch (op) {
            case eq:
            case neq:
            case gt:
            case ge:
            case lt:
            case le:
                val = expr.substring(s);
                break;
            case in:
            case nin:
                vals = Sets.newHashSet(Splitter.on(',')
                        .trimResults()
                        .split(expr.substring(s)));
                break;
            default:
                throw new RuntimeException("unsupport comparator");
        }

    }

    public boolean match(Row row) {
        String value = fn.apply(row, key);
        switch (op) {
            case eq:
                return (val.equals(value));
            case neq:
                return (!val.equals(value));
            case gt:
                return (val.compareTo(value) > 0);
            case ge:
                return (val.equals(value) || val.compareTo(value) > 0);
            case lt:
                return (val.compareTo(value) < 0);
            case le:
                return (val.equals(value) || val.compareTo(value) < 0);
            case in:
                return vals.contains(value);
            case nin:
                return !vals.contains(value);
        }

        return false;
    }

    @Override
    public String getCacheKey(Row row) {
        return key + "=" + fn.apply(row, key);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("expr", expr)
                .add("key", key)
                .add("op", op)
                .add("val", val)
                .add("vals", null == vals ? "null" : Joiner.on(',').useForNull("").join(vals))
                .toString();
    }

    public String getExpr() {
        return expr;
    }

    public String getKey() {
        return key;
    }

    static enum OP {
        in,
        nin,
        eq,
        neq,
        gt,
        ge,
        lt,
        le
    }

}
