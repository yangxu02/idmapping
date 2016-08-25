package com.linkx.mapping.function.dimension;

import com.linkx.mapping.row.MapBasedRows;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

/**
 * EchoDimExtractionFn Tester.
 *
 * @author <ulyx.yang@yeahmobi.com>
 * @since <pre>11/13/2014</pre>
 * @version 1.0
 */
public class EchoDimExtractionFnTest {

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
        DimensionExtractionFn fn = new EchoDimExtractionFn();
        Assert.assertEquals("imei123", fn.apply(MapBasedRows.row1.getDimensionVal("imei")));
    }

    /**
     *
     * Method: apply(Row row)
     *
     */
    @Test
    public void testApplyRow() throws Exception {
        DimensionExtractionFn fn = new EchoDimExtractionFn();
        Assert.assertEquals("", fn.apply(MapBasedRows.row1));
    }


} 
