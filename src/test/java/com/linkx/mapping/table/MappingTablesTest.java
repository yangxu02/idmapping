package com.linkx.mapping.table;

import com.linkx.mapping.table.hbase.HBaseMappingTables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
* HBaseMappingTables Tester.
* 
* @author <YangXu> 
* @since <pre>Dec 8, 2014</pre> 
* @version 1.0 
*/ 
public class MappingTablesTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: find(String key, String table) 
* 
*/ 
@Test
public void testFindForKeyTable() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: find(String key, String table, HConnection connection) 
* 
*/ 
@Test
public void testFindForKeyTableConnection() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: update(String key, String table, String user, HConnection connection) 
* 
*/ 
@Test
public void testUpdate() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: findByProbing(String key, String[] path, HConnection connection, Integer index) 
* 
*/ 
@Test
public void testFindByProbing() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: findByChain(String key, String[] path, HConnection connection, Integer index) 
* 
*/ 
@Test
public void testFindByChain() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getSplits(long min_id, long max_id, int chunks) 
* 
*/ 
@Test
public void testGetSplits() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: preSplit(int nRegions) 
* 
*/ 
@Test
public void testPreSplit() throws Exception {
    HBaseMappingTables.preSplit(1);
    HBaseMappingTables.preSplit(9);
    HBaseMappingTables.preSplit(10);
    HBaseMappingTables.preSplit(110);
    HBaseMappingTables.preSplit(100);
}


/** 
* 
* Method: createTable(String table, HConnection connection) 
* 
*/ 
@Test
public void testCreateTable() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = HBaseMappingTables.getClass().getMethod("createTable", String.class, HConnection.class);
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
* Method: isTableExists(String table, HConnection connection) 
* 
*/ 
@Test
public void testIsTableExists() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = HBaseMappingTables.getClass().getMethod("isTableExists", String.class, HConnection.class);
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
* Method: checkAndCreateTable(String table, HConnection connection) 
* 
*/ 
@Test
public void testCheckAndCreateTable() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = HBaseMappingTables.getClass().getMethod("checkAndCreateTable", String.class, HConnection.class);
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

} 
