package com.linkx.mapping.function.dimension;

import com.google.common.base.Joiner;
import com.linkx.mapping.row.MapBasedRows;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

/**
 * CsvPackingDimExtractionFn Tester.
 *
 * @author <ulyx.yang@yeahmobi.com>
 * @since <pre>11/12/2014</pre>
 * @version 1.0
 */
public class CsvPackingDimExtractionFnTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: getSeparator()
     *
     */
    @Test
    public void testGetSeparator() throws Exception {
        CsvPackingDimExtractionFn fn = new CsvPackingDimExtractionFn();
        Assert.assertEquals('\001', fn.getSeparator());

        fn = new CsvPackingDimExtractionFn(',');
        Assert.assertEquals(',', fn.getSeparator());
    }

    /**
     *
     * Method: apply(String input)
     *
     */
    @Test
    public void testApplyInput() throws Exception {
        DimensionExtractionFn fn = new CsvPackingDimExtractionFn();
        Assert.assertEquals("", fn.apply("abcd"));
    }

    /**
     *
     * Method: apply(Row row)
     *
     */
    @Test
    public void testApplyRow() throws Exception {
        DimensionExtractionFn fn = new CsvPackingDimExtractionFn();
        String res = Joiner.on('\001').join(MapBasedRows.row1.getValues());
        Assert.assertEquals(res, fn.apply(MapBasedRows.row1));

        fn = new CsvPackingDimExtractionFn(new String[]{"imei"});
        Assert.assertEquals(MapBasedRows.row1.getDimensionVal("imei"), fn.apply(MapBasedRows.row1));
    }


} 
