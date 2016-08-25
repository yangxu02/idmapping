package com.linkx.mapping.row;

import com.linkx.mapping.context.DimensionInfo;
import com.linkx.mapping.function.row.CompoundRowKey;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* CompoundRowKey Tester. 
* 
* @author <ulyx.yang@yeahmobi.com> 
* @since <pre>11/12/2014</pre> 
* @version 1.0 
*/ 
public class CompoundRowKeyTest { 

    DimensionInfo[] dims = new DimensionInfo[] {
            new DimensionInfo(),
    };

    DimensionInfo primary = new DimensionInfo();

    @Before
    public void before() throws Exception {
        primary.setName("ip");
        dims[0].setName("imei");
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: getDims()
     *
     */
    @Test
    public void testGetDims() throws Exception {
        CompoundRowKey rowKey = new CompoundRowKey(primary, dims, '|');
        Assert.assertArrayEquals(rowKey.getDims(), dims);
    }

    /**
     *
     * Method: apply(Row row)
     *
     */
    @Test
    public void testApply() throws Exception {
        Row row = MapBasedRows.row1;
        String imei = row.getDimensionVal("imei");
        String ip = row.getDimensionVal("ip");
        CompoundRowKey rowKey = new CompoundRowKey(primary, dims, '|');
        System.out.println(imei + "|" + ip);
        System.out.println(rowKey.apply(row));
        Assert.assertEquals(imei + "|" + ip, rowKey.apply(row));

        rowKey = new CompoundRowKey(new DimensionInfo(), new DimensionInfo[]{}, '|');
        Assert.assertNotEquals("", rowKey.apply(row));

        rowKey = new CompoundRowKey(primary, new DimensionInfo[]{}, '|');
        Assert.assertEquals(ip, rowKey.apply(row));
    }


} 
