package com.linkx.mapping.function.dimension;

import com.linkx.mapping.function.Hasher;
import com.linkx.mapping.function.dimension.AndDimExtractionFn;
import com.linkx.mapping.function.dimension.DimensionExtractionFn;
import com.linkx.mapping.function.dimension.HashingDimExtractionFn;
import com.linkx.mapping.function.dimension.JsonMapDimExtractionFn;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/** 
* AndDimExtractionFn Tester. 
* 
* @author <YangXu> 
* @since <pre>Mar 12, 2015</pre> 
* @version 1.0 
*/ 
public class AndDimExtractionFnTest { 

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
//TODO: Test goes here...
    Map<String, String> data = new HashMap<>();
    String input = "{\"mac_sha1\":\"65bce77cd1b3bbbf2f9d93e628b5d04973948395\",\"os\":\"android\",\"installed_at\":\"1421007658\",\"androidid\":\"10f3b62c865c2dd6\",\"sessions\":\"15\",\"tz\":\"UTC+0800\",\"source\":\"1\",\"osver\":\"15\",\"session_time\":\"4\",\"lang\":\"en\",\"mac_md5\":\"0b69734fbe8e231757292e09ae2b1f8e\",\"cc\":\"ph\"}";

    JsonMapDimExtractionFn fn1 = new JsonMapDimExtractionFn("androidid");
    HashingDimExtractionFn fn2 = new HashingDimExtractionFn(Hasher.md5, null, null);

    AndDimExtractionFn fn = new AndDimExtractionFn(new DimensionExtractionFn[]{fn1, fn2});

    System.out.println(fn.apply(input));
} 

/** 
* 
* Method: apply(Row row) 
* 
*/ 
@Test
public void testApplyRow() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: apply(Row row, String val) 
* 
*/ 
@Test
public void testApplyForRowVal() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getFns() 
* 
*/ 
@Test
public void testGetFns() throws Exception { 
//TODO: Test goes here... 
} 


} 
