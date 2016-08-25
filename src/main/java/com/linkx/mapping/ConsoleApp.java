package com.linkx.mapping;

import com.linkx.mapping.process.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class ConsoleApp {
    public static void main(String[] args) throws IOException {
        InputStreamReader isReader = new InputStreamReader(System.in);
        BufferedReader reader = new BufferedReader(isReader);
        String line = reader.readLine();

        if (null == line) {
            System.err.println("no input");
            System.exit(1);
        }

        boolean debug = true;
        String strDebug = System.getProperty("dmp.mapping.debug", "false");
        if ("false".equalsIgnoreCase(strDebug)) {
            debug = false;
        }

        System.err.println("mapping setup");
        Mapper mapper = new Mapper().setup();
        System.err.println("mapping start");
        while (null != line) {
            if (line.isEmpty()) {
                System.err.println("skip null input");
            }
            if (debug) System.out.println(line);
            mapper.start(line);
            line = reader.readLine();
        }
        System.err.println("mapping cleanup");
        mapper.cleanup();
        System.err.println("mapping end");
        System.exit(0);
    }
}
