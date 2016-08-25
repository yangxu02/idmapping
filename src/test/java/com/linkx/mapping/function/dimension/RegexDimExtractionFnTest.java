package com.linkx.mapping.function.dimension;

import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.Input;
import com.linkx.mapping.function.dimension.AndDimExtractionFn;
import com.linkx.mapping.function.dimension.DimensionExtractionFn;
import com.linkx.mapping.function.dimension.RegexDimExtractionFn;
import com.linkx.mapping.function.dimension.TrimmingDimExtractionFn;
import com.linkx.mapping.row.MapBasedRow;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * RegexDimExtractionFn Tester.
 *
 * @author <YangXu>
 * @since <pre>Dec 4, 2014</pre>
 * @version 1.0
 */
public class RegexDimExtractionFnTest {

    Input input = new Input();
    Context ctx = new Context();
    String json = "{\"referer\":\"http://global.ymtrack.com/ymconv/conv?transaction_id=10253ca6a1c2c7b7d568852054373f&aff_id=23483&campaignId=149\"}";

    @Before
    public void before() throws Exception {
        input.setFormat("json");
        ctx.setInput(input);
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
        DimensionExtractionFn fn1 = new RegexDimExtractionFn("transaction_id=[^,&; ]*");
        DimensionExtractionFn fn2 = new TrimmingDimExtractionFn("transaction_id=", -1);
        String id = fn1.apply("http://global.ymtrack.com/ymconv/conv?transaction_id=10253ca6a1c2c7b7d568852054373f&aff_id=23483&campaignId=149");
        Assert.assertEquals(id, "transaction_id=10253ca6a1c2c7b7d568852054373f");

        DimensionExtractionFn fn = new AndDimExtractionFn(new DimensionExtractionFn[] {fn1, fn2});
        id = fn.apply("http://global.ymtrack.com/ymconv/conv?transaction_id=10253ca6a1c2c7b7d568852054373f&aff_id=23483&campaignId=149");
        Assert.assertEquals(id, "10253ca6a1c2c7b7d568852054373f");
    }

    /**
     *
     * Method: apply(Row row)
     *
     */
    @Test
    public void testApplyRow() throws Exception {
        DimensionExtractionFn fn1 = new RegexDimExtractionFn("transaction_id=[^,&; ]*");
        String id = fn1.apply(new MapBasedRow(json, ctx));
        Assert.assertEquals("", id);
    }

    /**
     *
     * Method: apply(Row row, String dim)
     *
     */
    @Test
    public void testApplyForRowDim() throws Exception {
        DimensionExtractionFn fn1 = new RegexDimExtractionFn("transaction_id=[^,&; ]*");
        String id = fn1.apply(new MapBasedRow(json, ctx), "referer");
        Assert.assertEquals(id, "transaction_id=10253ca6a1c2c7b7d568852054373f");
    }

    /**
     *
     * Method: getRegex()
     *
     */
    @Test
    public void testGetRegex() throws Exception {
        RegexDimExtractionFn fn1 = new RegexDimExtractionFn("transaction_id=[^,&; ]*");
        Assert.assertEquals(fn1.getRegex(), "transaction_id=[^,&; ]*");
    }


} 
