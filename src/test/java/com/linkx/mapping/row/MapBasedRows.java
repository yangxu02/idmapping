package com.linkx.mapping.row;

import com.google.gson.Gson;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.Input;
import com.linkx.mapping.row.MapBasedRow;
import com.linkx.mapping.row.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * MapBasedRow Tester.
 *
 * @author <ulyx.yang@yeahmobi.com>
 * @since <pre>11/12/2014</pre>
 * @version 1.0
 */
public class MapBasedRows {

    static String csv = "imei123,androidid123,127.0.0.1,curl,cn,123456,eventtest,123,meta0,htc";
    static String json = "{\"imei\":\"imei123\", \"androidid\":\"id123\", \"ip\":\"127.0.0.1\", \"ua\":\"curl\", \"country\":\"cn\", \"timestamp\":\"1213\", \"eventname\":\"eventtest\"," +
            "\"cost\":\"123\", \"meta\":\"abc\", \"brand\":\"htc\"}";
    static String[] dimensions = new String[] {
            "imei", "androidid", "ip", "ua", "country", "timestamp", "eventname",
            "cost", "meta", "brand"
    };

    static String meta1 = "imei123|androidid123|127.0.0.1|curl|cn|123456|eventtest|123|{\"price\":123,\"pid\":1}|htc";
    static String meta2 = "{\"imei\":\"imei123\", \"androidid\":\"id123\", \"ip\":\"127.0.0.1\", \"ua\":\"curl\", \"country\":\"cn\", \"timestamp\":1213, \"eventname\":\"eventtest\",\n" +
            "\"cost\":123,\"meta\":\"{\\\"price\\\":123,\\\"pid\\\":1}\", \"brand\":\"htc\"}";

    static Context ctx = new Context();
    static Context ctx2 = new Context();

    public static Row row1;
    public static Row row2;
    public static Row rowMeta1;
    public static Row rowMeta2;

    static {
        Input input = new Input();
        input.setDelim(",");
        input.setFormat("csv");
        input.setDimensions(dimensions);
        ctx.setInput(input);

        Input input2 = new Input();
        input2.setDelim(",");
        input2.setFormat("json");
        input2.setDimensions(dimensions);
        ctx2.setInput(input2);

        row1 = new MapBasedRow(csv, ctx);
        row2 = new MapBasedRow(json, ctx2);
        rowMeta2 = new MapBasedRow(meta2, ctx2);
        input.setDelim("|");
        rowMeta1 = new MapBasedRow(meta1, ctx);
        input.setDelim(",");
    }

    @Test
    public void test() {
        System.out.println(rowMeta2.getDimensionVal("timestamp"));
    }

} 
