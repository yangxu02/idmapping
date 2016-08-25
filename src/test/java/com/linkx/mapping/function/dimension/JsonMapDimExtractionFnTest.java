package com.linkx.mapping.function.dimension;

import com.google.gson.Gson;
import com.linkx.mapping.row.MapBasedRows;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.util.HashMap;
import java.util.Map;

/** 
* JsonMapDimExtractionFn Tester. 
* 
* @author <ulyx.yang@yeahmobi.com> 
* @since <pre>11/13/2014</pre> 
* @version 1.0 
*/ 
public class JsonMapDimExtractionFnTest {

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
        Map<String, String> test = new HashMap<>();
        test.put("abc", "123");

        Map<String, Object> test2 = new HashMap<>();
        test2.put("price", 123);
        test2.put("pid", 1);

        test.put("meta", new Gson().toJson(test2));
        System.out.println(new Gson().toJson(test));

        JsonMapDimExtractionFn fn = new JsonMapDimExtractionFn("price");
        System.out.println(MapBasedRows.rowMeta2.getDimensionVal("meta"));
        System.out.println(new Gson().toJson(MapBasedRows.rowMeta2));
        System.out.println(new Gson().toJson(MapBasedRows.rowMeta1));
        Assert.assertEquals(fn.apply(MapBasedRows.rowMeta2.getDimensionVal("meta")), "123");
        Assert.assertEquals(fn.apply(MapBasedRows.rowMeta1.getDimensionVal("meta")), "123");
        fn = new JsonMapDimExtractionFn("pid");
        Assert.assertEquals(fn.apply(MapBasedRows.rowMeta2.getDimensionVal("meta")), "1");
    }

    /**
     *
     * Method: apply(Row row)
     *
     */
    @Test
    public void testApplyRow() throws Exception {
        JsonMapDimExtractionFn fn = new JsonMapDimExtractionFn("price");
        Assert.assertEquals(fn.apply(MapBasedRows.rowMeta1), "");
    }

    /**
     *
     * Method: getKey()
     *
     */
    @Test
    public void testGetKey() throws Exception {
        JsonMapDimExtractionFn fn = new JsonMapDimExtractionFn("price");
        Assert.assertEquals(fn.getKey(), "price");
    }


} 
