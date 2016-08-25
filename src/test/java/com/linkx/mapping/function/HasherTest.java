package com.linkx.mapping.function;

import com.linkx.mapping.function.Hasher;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* Hasher Tester. 
* 
* @author <YangXu> 
* @since <pre>Mar 12, 2015</pre> 
* @version 1.0 
*/ 
public class HasherTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: hash(String input) 
* 
*/ 
@Test
public void testHash() throws Exception {
    System.out.println(Hasher.md5.hash("10f3b62c865c2dd6"));
}


} 
