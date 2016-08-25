package com.linkx.mapping.filter;

import com.google.common.base.Strings;

/**
 * Created by yangxu on 1/20/15.
 */

public class Expressions {

    public static Expression create(String expr) {
        if (Strings.isNullOrEmpty(expr)) return null;
        if (expr.contains("&&")) {
            return new AndExpression(expr);
        } else if (expr.contains("||")) {
            return new OrExpression(expr);
        } else {
            return new SimpleExpression(expr);
        }

    }


}
