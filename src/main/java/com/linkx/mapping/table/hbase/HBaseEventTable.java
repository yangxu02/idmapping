package com.linkx.mapping.table.hbase;

import com.google.common.base.Strings;
import com.linkx.mapping.payload.PayLoad;
import com.linkx.hbase.RowKeyGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class HBaseEventTable {
    private final static Log logger = LogFactory.getLog(HBaseEventTable.class);
    private final static byte[] TableName = Bytes.toBytes("dmp_user_event");
    private final static byte[] Family = Bytes.toBytes("f");
    private final static RowKeyGenerator rowKeyGenerator = RowKeyGenerator.hash;
    private final static String nRegions = "100";

    public static boolean insert(String key, PayLoad payLoad, HConnection connection) {

        if (Strings.isNullOrEmpty(key)
                || null == payLoad
                || null == payLoad.getData()
                || payLoad.getData().isEmpty()) {
            logger.info("no key or payload given, skip");
            return true;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[method=insert],[key=" + key + "],[payload="
                    + payLoad + "]");
        }

        byte[] family = Family;
        if (!Strings.isNullOrEmpty(payLoad.getColumnFamily())) {
            family = Bytes.toBytes(payLoad.getColumnFamily());
        }

        Put put = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, Object> entry : payLoad.getData().entrySet()) {
            put.add(family, Bytes.toBytes(entry.getKey()),
                    Bytes.toBytes((String) entry.getValue()));
        }


        byte[] table = TableName;
        if (!Strings.isNullOrEmpty(payLoad.getTable())) {
            table = Bytes.toBytes(payLoad.getTable());
        }
        try {
            HTableInterface hTable = connection.getTable(table);
            hTable.put(put);
            hTable.flushCommits();
            hTable.close();
            logger.info("[action=insert],[key=" + key + "][payload=" + payLoad + "]");
        } catch (IOException e) {
            // just skip
            logger.error("", e);
        }
        return true;
    }

    public static boolean insert(String user, String keySalt,
                                 PayLoad payLoad, HConnection connection) {
        if (Strings.isNullOrEmpty(user)
                || null == payLoad
                || payLoad.getData().isEmpty()
                ) {
            logger.info("no user or payload given, skip");
            return true;
        }


        if (logger.isDebugEnabled()) {
            logger.debug("[method=insert],[user=" + user + "],[keysalt=" + keySalt + "],[payload="
                    + payLoad + "]");
        }

        byte[] rowKey = rowKeyGenerator.from(user, nRegions);
        String key = new String(rowKey);
        if (!Strings.isNullOrEmpty(keySalt)) {
            key += ('|' + keySalt);
        }
        return insert(key, payLoad, connection);
    }

    public static boolean insert(PayLoad payLoad, HConnection connection) {
        if (null == payLoad
                || Strings.isNullOrEmpty(payLoad.getRowKey())
                || null == payLoad.getData()
                || payLoad.getData().isEmpty()
                ) {
            logger.info("no user or payload given, skip");
            return true;
        }
        String user = payLoad.getRowKey();
        String keySalt = payLoad.getRowKeyPostfix();

        if (logger.isDebugEnabled()) {
            logger.debug("[method=insert],[user=" + user + "],[keysalt=" + keySalt + "],[payload="
                    + payLoad + "]");
        }

        byte[] rowKey = rowKeyGenerator.from(user, nRegions);
        String key = new String(rowKey);
        if (!Strings.isNullOrEmpty(keySalt)) {
            key += ('|' + keySalt);
        }
        return insert(key, payLoad, connection);
    }

}
