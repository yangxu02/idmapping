package com.linkx.mapping.payload;

import com.linkx.mapping.payload.TimeStampFormatter;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

/** 
* TimeStampFormatter Tester. 
* 
* @author <YangXu> 
* @since <pre>Dec 9, 2014</pre> 
* @version 1.0 
*/ 
public class TimeStampFormatterTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: format(String val) 
* 
*/ 
@Test
public void testFormat() throws Exception {
    String[] times = new String[] {
            "2014-12-07 01:00:00",
            "2014-12-07",
            "1418094628",
            "1997-07-16T19:20:30+01:00",
            "1997-07-16T19:20:30.45+01:00",
    };
    for (String time : times) {
        System.out.println(time + "->" + TimeStampFormatter.format(time));
    }
    System.out.println("1418094628" + "->" + new DateTime(1418094628l*1000));
    System.out.println(new Date().getTime());
}


} 
