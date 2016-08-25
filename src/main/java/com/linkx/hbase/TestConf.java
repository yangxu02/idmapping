package com.linkx.hbase;
/**
 * Created by yangxu on 11/6/14.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestConf {

    private static Logger logger = Logger.getLogger(TestConf.class);

    public static void main(String[] args) throws JsonProcessingException {

        Map<String, Object> result = new HashMap<>();
        result.put("test1", "val1");
        result.put("test2", "val2");
        result.put("test3", new String[]{"val2"});

        System.out.println(new ObjectMapper().writeValueAsString(result));

        Configuration conf = HBaseUtil.getConf();

        Iterator it = conf.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
            if (entry.getKey().contains("yeahmobi")) {
                System.out.println(entry.getKey() + " --> " + entry.getValue());
            }
        }
    }
}
