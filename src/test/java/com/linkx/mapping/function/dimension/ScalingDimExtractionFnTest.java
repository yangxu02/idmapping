package com.linkx.mapping.function.dimension;

import com.linkx.mapping.row.MapBasedRows;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.math.BigDecimal;

/**
 * ScalingDimExtractionFn Tester.
 *
 * @author <ulyx.yang@yeahmobi.com>
 * @since <pre>11/13/2014</pre>
 * @version 1.0
 */
public class ScalingDimExtractionFnTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: apply(String input)
     *
     */
    @Test
    public void testApplyInput() throws Exception {
        ScalingDimExtractionFn fn = new ScalingDimExtractionFn(new BigDecimal(100));
        Assert.assertEquals(12300 + "", fn.apply(MapBasedRows.row1, "cost"));
        Assert.assertEquals(12300 + "", fn.apply("123"));
    }

    /**
     *
     * Method: apply(Row row)
     *
     */
    @Test
    public void testApplyRow() throws Exception {
    }

    /**
     *
     * Method: apply(Row row, String val)
     *
     */
    @Test
    public void testApplyForRowVal() throws Exception {
    }

    /**
     *
     * Method: getScale()
     *
     */
    @Test
    public void testGetScale() throws Exception {
    }


} 
