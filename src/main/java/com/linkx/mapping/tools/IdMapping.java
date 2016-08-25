package com.linkx.mapping.tools;
/**
 * Created by ulyx.yang@ndpmedia.com on 3/2/15.
 */

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.gson.Gson;
import com.linkx.mapping.table.hbase.HBaseMappingTables;
import com.linkx.mapping.table.hbase.HBaseUserTable;
import com.linkx.hbase.HBaseUtil;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IdMapping {

    private static Logger logger = Logger.getLogger(IdMapping.class);

    public static void main(String[] args) throws IOException {
        String inFile = args[0];
        String outFile = args[1];

        final HConnection connection = HBaseUtil.getConnection();

        Files.readLines(new File(inFile), Charsets.UTF_8, new LineProcessor<Object>() {
            @Override
            public boolean processLine(String s) throws IOException {
                if (null == s) return false;
                if (s.isEmpty()) return true;

                String[] fields = s.split(",");

                String seed = fields[0];

                String uid = "";
                for (int i = 1; i < fields.length; ++i) {
                    uid = HBaseMappingTables.find(seed, fields[i], connection);
                    if (!Strings.isNullOrEmpty(uid)) break;
                }

                if (!Strings.isNullOrEmpty(uid)) {
                    Map<String, String> idMaps = new HashMap<>();
                    Set<String> alisas = new HashSet<>();
                    HBaseUserTable.getDevices(uid, connection, idMaps, alisas);
                    logger.info(new Gson().toJson(idMaps));
                }
                return true;
            }

            @Override
            public Object getResult() {
                return null;
            }
        });

    }


}
