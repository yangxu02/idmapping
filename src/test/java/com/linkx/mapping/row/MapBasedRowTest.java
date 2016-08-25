package com.linkx.mapping.row;

import com.google.gson.Gson;
import com.google.inject.Guice;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.Input;
import com.linkx.mapping.row.MapBasedRow;
import com.linkx.mapping.row.Row;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

/**
 * MapBasedRow Tester.
 *
 * @author <ulyx.yang@yeahmobi.com>
 * @since <pre>11/12/2014</pre>
 * @version 1.0
 */
public class MapBasedRowTest {

    String csv = "imei123,androidid123,127.0.0.1,curl,cn,123456,eventtest,123,meta0,htc";
    String json = "{\"imei\":\"imei123\", \"androidid\":\"id123\", \"ip\":\"127.0.0.1\", \"ua\":\"curl\", \"country\":\"cn\", \"timestamp\":\"1213\", \"eventname\":\"eventtest\"," +
            "\"cost\":\"123\", \"meta\":\"abc\", \"brand\":\"htc\"}";
    String[] dimensions = new String[] {
            "imei", "androidid", "ip", "ua", "country", "timestamp", "eventname",
            "cost", "meta", "brand"
    };
    Context ctx = new Context();
    Context ctx2 = new Context();

    @Before
    public void before() throws Exception {
        Input input = new Input();
        input.setDelim(",");
        input.setFormat("csv");
        input.setDimensions(dimensions);
        ctx.setInput(input);
        System.out.println(ctx + "ctx" + "->" + new Gson().toJson(ctx));

        Input input2 = new Input();
        input2.setDelim(",");
        input2.setFormat("json");
        input2.setDimensions(dimensions);
        ctx2.setInput(input2);
        System.out.println(ctx2 + "ctx2" + "->" + new Gson().toJson(ctx));
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: getDimensionVal(String dim)
     *
     */
    @Test
    public void testGetDimensionVal() throws Exception {
        System.out.println(new Gson().toJson(ctx));
        Row row = new MapBasedRow(csv, ctx);
        Assert.assertTrue("imei123".equalsIgnoreCase(row.getDimensionVal("imei")));

        row = new MapBasedRow(json, ctx2);
        Assert.assertTrue("imei123".equalsIgnoreCase(row.getDimensionVal("imei")));
    }

    /**
     *
     * Method: getLongVal(String dim)
     *
     */
    @Test
    public void testGetLongVal() throws Exception {
        System.out.println(new Gson().toJson(ctx));
        Row row = new MapBasedRow(csv, ctx);
        Assert.assertTrue(123 == row.getLongVal("cost"));

        row = new MapBasedRow(json, ctx2);
        Assert.assertTrue(123 == row.getLongVal("cost"));
    }

    /**
     *
     * Method: getValues()
     *
     */
    @Test
    public void testGetValues() throws Exception {
        System.out.println(new Gson().toJson(ctx));
        Row row = new MapBasedRow(csv, ctx);
        System.out.println(new Gson().toJson(row.getValues()));

        row = new MapBasedRow(json, ctx2);
        System.out.println(new Gson().toJson(row.getValues()));
    }


    /**
     *
     * Method: loadFromCsv(String input, Context context)
     *
     */
    @Test
    public void testLoadFromCsv() throws Exception {
/*
try { 
   Method method = MapBasedRow.getClass().getMethod("loadFromCsv", String.class, Context.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     *
     * Method: loadFromJson(String input, Context context)
     *
     */
    @Test
    public void testLoadFromJson() throws Exception {
/*
try { 
   Method method = MapBasedRow.getClass().getMethod("loadFromJson", String.class, Context.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

} 
