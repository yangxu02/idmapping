package com.linkx.mapping.table.hbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.linkx.mapping.payload.PayLoad;
import com.linkx.mapping.process.Linkage;
import com.linkx.mapping.process.Result;
import com.linkx.mapping.process.UserLinkage;
import com.linkx.hbase.RowKeyGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class HBaseUserTable {

    private final static Log logger = LogFactory.getLog(HBaseUserTable.class);
    private final static byte[] TableName = Bytes.toBytes("dmp_user_profile");
    private final static byte[] CounterFamily = Bytes.toBytes("stats");
    private final static byte[] ProfileFamily = Bytes.toBytes("info");
    private final static byte[] AliasFamily = Bytes.toBytes("alias");
    private final static byte[] DeviceFamily = Bytes.toBytes("dv");
    private final static RowKeyGenerator rowKeyGenerator = RowKeyGenerator.hash;
    private final static String nRegions = "25";
    private final static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * TODO bi-direction user link?
     * update user aliases(user id previously used)
     * row | ------------user0--------------
     * cf  | ------------alias--------------
     * col | user1 | user2 | user3 | user4 |
     * val |   ""  |   ""  |   ""  |   ""  |
     * @param userLinkage: user linkage
     * @return true if success, false if error
     */
    public static boolean updateAliases(UserLinkage userLinkage, HConnection connection,Result result) {
        return updateAliases(userLinkage, Bytes.toString(TableName), Bytes.toString(AliasFamily), connection,result);
    }

    public static boolean updateAliases(UserLinkage userLinkage, String table, String cf, HConnection connection,Result result) {

        List<Put> puts = getAliases(userLinkage, cf);
        if (null == puts || puts.isEmpty()) {
            logger.info("[action=updateAliases],[msg=no user linkage info given, skip]");
            return true;
        }

        byte[] tableName = TableName;
        if (!Strings.isNullOrEmpty(table)) {
            tableName = Bytes.toBytes(table);
        }
        //result.addPuts(puts);
        for(Put put:puts){
        	result.addRow(put);
        }
        boolean success = true;
        //boolean success = update(puts, tableName, connection);
        logger.info("[action=updateAlias]"+puts.size()+"[result=" + success + "][users=" + userLinkage.getUserId() + "][payload="
                + userLinkage + "]");
        return success;
    }


    private static boolean update(List<Put> puts, byte[] table, HConnection connection) {
        try {
//            HConnection connection = HBaseUtil.getConnection();
            HTableInterface hTable = connection.getTable(table);
            hTable.put(puts);
            hTable.flushCommits();
            hTable.close();
//            HBaseUtil.releaseConnection(connection);
            return true;
        } catch (IOException e) {
            // just skip
            logger.error("", e);
        }
        return false;
    }


    private static boolean update(List<Put> puts, List<Increment> incs, byte[] table, HConnection connection) {
        try {
//            HConnection connection = HBaseUtil.getConnection();
            HTableInterface hTable = connection.getTable(table);
            hTable.put(puts);
            for (int i = 0; i < incs.size(); ++i) {
                hTable.increment(incs.get(i));
            }
            hTable.flushCommits();
            hTable.close();
//            HBaseUtil.releaseConnection(connection);
            return true;
        } catch (IOException e) {
            // just skip
            logger.error("", e);
        }
        return false;
    }

    private static List<Put> getAliases(UserLinkage userLinkage, String cf) {
           if (null == userLinkage
                   || null == userLinkage.getLinkages()
                   || userLinkage.getLinkages().isEmpty()) {
            logger.info("[action=updateAliases],[msg=no user linkage info given, skip]");
            return null;
        }

        Set<String> users = new HashSet<>();
        String user = userLinkage.getUserId();
        users.add(user);
        for (Linkage linkage : userLinkage.getLinkages()) {
            String alias = linkage.getUserId();
            if (Strings.isNullOrEmpty(alias)
                    || alias.equals(user)) continue;
            users.add(alias);
        }

        if (users.size() <= 1) {
            logger.info("[action=updateAliases],[msg=no additional user linkage info given, skip]");
            return null;
        }


        byte[] family = AliasFamily;
        if (!Strings.isNullOrEmpty(cf)) {
            family = Bytes.toBytes(cf);
        }
         byte[] val = Bytes.toBytes("");
        List<Put> puts = new ArrayList<>();
        for (String cur : users) {
            byte[] rowKey = rowKeyGenerator.from(cur, nRegions);
            Put put = new Put(rowKey);
            for (String other : users) {
                put.add(family, Bytes.toBytes(other), val);
            }
            puts.add(put);
        }

        return puts;
    }


    private static List<Put> getAliases(UserLinkage userLinkage) {
        return getAliases(userLinkage, Bytes.toString(AliasFamily));
    }

    /**
     * update user devices(user id previously used)
     * row | ------------user0------------------
     * cf  | -----------devices-----------------
     * col | a\001b | a\001x | a\001yyyyy | a\001h |
     * val |  imei  | idfa   |  androidid |   mac  |
     * @param user: user to update
     * @param payLoad: device infos to update
     * @return true if success, false if error
     */
    public static boolean updateDevices(String user, PayLoad payLoad, HConnection connection,Result resul) {
        // TODO
        if (Strings.isNullOrEmpty(user)
                || null == payLoad
                || null == payLoad.getData()
                || payLoad.getData().isEmpty()) {
            logger.info("no user or event counters given, skip");
            return true;
        }

        byte[] family = DeviceFamily;
        if (!Strings.isNullOrEmpty(payLoad.getColumnFamily())) {
            family = Bytes.toBytes(payLoad.getColumnFamily());
        }
        byte[] table = TableName;
        if (!Strings.isNullOrEmpty(payLoad.getTable())) {
            table = Bytes.toBytes(payLoad.getTable());
        }

        byte[] rowKey = rowKeyGenerator.from(user, nRegions);
        Put put = new Put(rowKey);
        for (Map.Entry<String, Object> entry : payLoad.getData().entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            put.add(family, Bytes.toBytes(key), Bytes.toBytes((String) val));
        }
        resul.addRow(put);
        boolean success = true;
        //boolean success = update(Lists.newArrayList(put), table, connection);
        logger.info("[action=updateDevices],[users=" + user + "],[payload="
                + payLoad + "]");
        return success;
    }

    public static boolean getDevices(String user, HConnection connection, Map<String, String> devices, Set<String> alisas) {
        return getDevices(user, TableName, DeviceFamily, AliasFamily, connection, devices, alisas);
    }

    public static boolean getDevices(String user, byte[] tableName, byte[] dcf, byte[] acf, HConnection connection, Map<String, String> devices, Set<String> alisas) {
        // TODO
        if (Strings.isNullOrEmpty(user)) {
            logger.info("no user or event counters given, skip");
            return true;
        }

        if (alisas.contains(user)) {
            logger.info("user " + user + " already scanned, skip");
            return true;
        }

        byte[] deviceFamily = DeviceFamily;
        if (null != dcf && dcf.length != 0) {
            deviceFamily = dcf;
        }
        byte[] aliasFamily = AliasFamily;
        if (null != acf && acf.length != 0) {
            aliasFamily = acf;
        }
        byte[] table = TableName;
        if (null != tableName && tableName.length != 0) {
            table = tableName;
        }

        alisas.add(user);
        byte[] rowKey = rowKeyGenerator.from(user, nRegions);
        Get get = new Get(rowKey);
        get.addFamily(deviceFamily);
        get.addFamily(aliasFamily);

        try {
            logger.info("start: getting devices from user " + user);
            HTableInterface hTable = connection.getTable(table);
            org.apache.hadoop.hbase.client.Result result = hTable.get(get);
            String id;
            String type;
            for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(deviceFamily).entrySet()) {
                id = Bytes.toString(entry.getKey());
                type = Bytes.toString(entry.getValue());
                devices.put(id, type);
            }

            for (byte[] alisa : result.getFamilyMap(aliasFamily).keySet()) {
                getDevices(Bytes.toString(alisa), table, deviceFamily, aliasFamily, connection, devices, alisas);
            }

            logger.info("done: getting devices from user " + user);
        } catch (IOException e) {
            logger.error("", e);
        }

        return true;
    }

    /**
     * update user event counters(user id previously used)
     * row | ------------user0------------------
     * cf  | ------------stats------------------
     * col | click@sum | click@last | click@cost |
     * val |    1000   |  1233455   |    2000    |
     * @param user: user to update
     * @param payLoad: device infos to update
     * @return true if success, false if error
     */
    public static boolean updateEventCounters(String user, PayLoad payLoad, HConnection connection,Result result) {
        // TODO
        if (Strings.isNullOrEmpty(user)
                || null == payLoad
                || null == payLoad.getData()
                || payLoad.getData().isEmpty()) {
            logger.info("no user or event counters given, skip");
            return true;
        }

        byte[] family = CounterFamily;
        if (!Strings.isNullOrEmpty(payLoad.getColumnFamily())) {
            family = Bytes.toBytes(payLoad.getColumnFamily());
        }
        byte[] table = TableName;
        if (!Strings.isNullOrEmpty(payLoad.getTable())) {
            table = Bytes.toBytes(payLoad.getTable());
        }

        byte[] rowKey = rowKeyGenerator.from(user, nRegions);
        Put put = new Put(rowKey);
        Increment inc = new Increment(rowKey);
        for (Map.Entry<String, Object> entry : payLoad.getData().entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (key.endsWith("@last")) { // time field
                put.add(family, Bytes.toBytes(key), Bytes.toBytes((long)val));
            } else {
                inc.addColumn(family, Bytes.toBytes(key), (long) val);
            }
        }
        result.addRow(put);
        result.addRow(inc);
        boolean success  = batchUpdate(result.getPuts(), table, connection);
        //boolean success = update(Lists.newArrayList(put), Lists.newArrayList(inc), table, connection);
        logger.info("[action=updateCounters][success=" + success + "][user=" + user + "],[payload="
                + payLoad + "]");
        return success;
    }
    
    public static boolean batchUpdate(List<Row> lists,byte[] table ,HConnection connection){
    	boolean success = false;
    	
    	 try {
			HTableInterface hTable = connection.getTable(table);
			org.apache.hadoop.hbase.client.Result[] results = new org.apache.hadoop.hbase.client.Result[lists.size()];
			hTable.batch(lists, results);
			hTable.flushCommits();
	        hTable.close();
	        success = true;
		} catch (Exception e) {
			logger.error("", e);
		}
    	
    	return success;
    }

    /**
     * update user profile(user id previously used)
     * row | ------------user0------------------
     * cf  | ------------stats------------------
     * col | click@sum | click@last | click@cost |
     * val |    1000   |  1233455   |    2000    |
     * @param user: user to update
     * @param payLoad: device infos to update
     * @return true if success, false if error
     */
    public static boolean updateProfile(String user, PayLoad payLoad, HConnection connection,Result result) {
        // TODO
        if (Strings.isNullOrEmpty(user)
                || null == payLoad
                || null == payLoad.getData()
                || payLoad.getData().isEmpty()) {
            logger.info("no user or event counters given, skip");
            return true;
        }

        byte[] family = CounterFamily;
        if (!Strings.isNullOrEmpty(payLoad.getColumnFamily())) {
            family = Bytes.toBytes(payLoad.getColumnFamily());
        }
        byte[] table = TableName;
        if (!Strings.isNullOrEmpty(payLoad.getTable())) {
            table = Bytes.toBytes(payLoad.getTable());
        }
        byte[] rowKey = rowKeyGenerator.from(user, nRegions);
        Put put = new Put(rowKey);
        for (Map.Entry<String, Object> entry : payLoad.getData().entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (val instanceof String) {
                put.add(family, Bytes.toBytes(key), Bytes.toBytes((String)val));
            } else {
                put.add(family, Bytes.toBytes(key), Bytes.toBytes((long)val));
            }
        }
        result.addRow(put);
        boolean success = true;
        //boolean success = update(Lists.newArrayList(put), table, connection);
        logger.info("[action=updateProfiles][success=" + success + "][user=" + user + "],[payload="
                + payLoad + "]");
        return success;
    }
}
