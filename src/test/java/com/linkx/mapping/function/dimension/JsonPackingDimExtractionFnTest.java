package com.linkx.mapping.function.dimension;

import com.google.gson.Gson;
import com.linkx.mapping.row.MapBasedRows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/** 
* JsonPackingDimExtractionFn Tester. 
* 
* @author <ulyx.yang@yeahmobi.com> 
* @since <pre>11/13/2014</pre> 
* @version 1.0 
*/ 
public class JsonPackingDimExtractionFnTest {

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
        DimensionExtractionFn fn = new JsonPackingDimExtractionFn();
        Assert.assertEquals("", fn.apply("abcd"));
    }

    /**
     *
     * Method: apply(Row row)
     *
     */
    @Test
    public void testApplyRow() throws Exception {
        DimensionExtractionFn fn = new JsonPackingDimExtractionFn();
        String res = new Gson().toJson(MapBasedRows.row1.getData());
        Assert.assertEquals(res, fn.apply(MapBasedRows.row1));
        Set<String> filters = new HashSet<>();
        filters.add("imei");
        fn = new JsonPackingDimExtractionFn(filters);
        Assert.assertTrue(!fn.apply(MapBasedRows.row1).contains("imei"));
        System.out.println(fn.apply(MapBasedRows.row1));
    }


} 
