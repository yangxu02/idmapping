package com.linkx.mapping.filter;

import com.linkx.mapping.filter.Expressions;
import com.linkx.mapping.filter.SimpleExpression;
import com.linkx.mapping.row.SimpleMapBasedRow;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SimpleExpression Tester.
 *
 * @author <YangXu>
 * @since <pre>Jan 20, 2015</pre>
 * @version 1.0
 */
public class ExpressionTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: match(Row row)
     *
     */
    @Test
    public void testMatch() throws Exception {
    }

    /**
     *
     * Method: toString()
     *
     */
    @Test
    public void testToString() throws Exception {
    }


    /**
     *
     * Method: compile()
     *
     */
    @Test
    public void testCompile() throws Exception {
        SimpleMapBasedRow row = new SimpleMapBasedRow();
        row.set("f1", "v1");
        row.set("f2", "v2");

        Assert.assertTrue(new SimpleExpression("f1 eq v1").match(row));
        Assert.assertFalse(new SimpleExpression("f1 neq v1").match(row));
        Assert.assertFalse(new SimpleExpression("f1 eq v2").match(row));
        Assert.assertTrue(new SimpleExpression("f1 neq v2").match(row));
        Assert.assertTrue(new SimpleExpression("f1 in v1, v2").match(row));
        Assert.assertFalse(new SimpleExpression("f1 nin v1, v2").match(row));

        System.out.println(new SimpleExpression("f1 nin v1, v2").getCacheKey(row));
        System.out.println(Expressions.create("f1 in v1, v2 && f2 eq v2").getCacheKey(row));
        Assert.assertTrue(Expressions.create("f1 in v1, v2 && f2 eq v2").match(row));
        Assert.assertTrue(Expressions.create("f1 nin v1, v2 || f2 eq v2").match(row));

    }

} 
