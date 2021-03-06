package com.linkx.mapping.table.hbase;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.linkx.hbase.HBaseUtil;
import com.linkx.hbase.RowKeyGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class HBaseMappingTables {
    private final static Log logger = LogFactory.getLog(HBaseMappingTables.class);
    public final static byte[] Family = Bytes.toBytes("u");
    public final static byte[] Quantifier = Bytes.toBytes("u");
    public final static RowKeyGenerator rowKeyGenerator = RowKeyGenerator.hash;
    public final static String nRegions = "25";
    private final static long MAX_FILESIZE = 1073741824;
    private final static int MAX_VERSIONS = 1;
    private final static Compression.Algorithm COMPRESSION = Compression.Algorithm.SNAPPY;


    /**
     * find user id from Table "table" with key "key"
     * @param key: identity key, as row key seed
     * @param table: table name
     * @return user id with given key, or "" if not found
     */
    public static String find(String key, String table) {
        if (Strings.isNullOrEmpty(key)
                || Strings.isNullOrEmpty(table)) {
            logger.info("no mapping key or table name given, skip");
            return "";
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[method=find],[key=" + key + "],[table="
                    + table + "]");
        }

        byte[] rowKey = rowKeyGenerator.from(key, nRegions);
        Get get = new Get(rowKey);
        get.addColumn(Family, Quantifier);

        boolean success = false;
        String res = "";
        try {
            HConnection connection = HBaseUtil.getConnection();
            // check if mapping table exists
//            checkAndCreateTable(table, connection);

            HTableInterface hTable = connection.getTable(table);
            Result result = hTable.get(get);
            if (null != result) {
                byte[] val = result.getValue(Family, Quantifier);
                if (null != val && val.length != 0) {
                    res = new String(val);
                }
            }
            hTable.close();
            HBaseUtil.releaseConnection(connection);
        } catch (IOException e) {
            // just skip
            logger.error("", e);
        }
        logger.info("[action=find][key=" + key + "][user=" + res + "][table="
                + table + "][success=" + success + "]");
        return res;
    }
    
    
    public static void batchFind(List<Get> gets,String table,HConnection connection,Result[] results){
    	
    	 StringBuffer sb = new StringBuffer();
    	 for(Get put:gets){
    		 sb.append(Bytes.toString(put.getRow())).append(",");
    	 }
    	logger.info("----batch ---" +gets.size()+"--gets from  table " + table+"----------"+sb.toString());
    	try {
			HTableInterface hTable = connection.getTable(table);
			for(Get get:gets){
				get.addColumn(Family, Quantifier);
			}
			
			hTable.batch(gets, results);
			hTable.close();
		} catch (IOException e) {
			logger.error("", e);
		} catch (InterruptedException e) {
			logger.error("", e);
		}
    	
    }

     public static String find(String key, String table, HConnection connection) {
         if (Strings.isNullOrEmpty(key)
                 || Strings.isNullOrEmpty(table)) {
             logger.info("no mapping key or table name given, skip");
             return "";
         }

         if (logger.isDebugEnabled()) {
             logger.debug("[method=find],[key=" + key + "],[table="
                     + table + "]");
         }

         byte[] rowKey = rowKeyGenerator.from(key, nRegions);
         Get get = new Get(rowKey);
         get.addColumn(Family, Quantifier);

         boolean success = false;
         String res = "";
         try {
             // check if mapping table exists
//             checkAndCreateTable(table, connection);

             HTableInterface hTable = connection.getTable(table);
             Result result = hTable.get(get);
             if (null != result) {
                 byte[] val = result.getValue(Family, Quantifier);
                 if (null != val && val.length != 0) {
                     res = new String(val);
                 }
             }
             hTable.close();
             success = true;
         } catch (IOException e) {
             // just skip
             logger.error("", e);
         }
         logger.info("[action=find][key=" + key + "][user=" + res + "][table="
                 + table + "][success=" + success + "]");
         return res;
     }
     
     public static String findWhithSuffix(String key, String table, HConnection connection,String rowKeySuffix,boolean addSuffix) {
         if (Strings.isNullOrEmpty(key)
                 || Strings.isNullOrEmpty(table)) {
             logger.info("no mapping key or table name given, skip");
             return "";
         }

         if (logger.isDebugEnabled()) {
             logger.debug("[method=find],[key=" + key + "],[table="
                     + table + "]");
         }
         byte[] rowKey = null;
         if(addSuffix){
        	 rowKey = Bytes.toBytes(key+rowKeySuffix);
        	 logger.info("---final find hbase key is ------->" + (key+rowKeySuffix));
         }else {
        	 rowKey = rowKeyGenerator.from(key, nRegions);
        	 logger.info("---final find hbase key is ------->" + Bytes.toString(rowKey));
         }
         Get get = new Get(rowKey);
         get.addColumn(Family, Quantifier);

         boolean success = false;
         String res = "";
         try {
             // check if mapping table exists
//             checkAndCreateTable(table, connection);

             HTableInterface hTable = connection.getTable(table);
             Result result = hTable.get(get);
             if (null != result) {
                 byte[] val = result.getValue(Family, Quantifier);
                 if (null != val && val.length != 0) {
                     res = new String(val);
                 }
             }
             hTable.close();
             success = true;
         } catch (IOException e) {
             // just skip
             logger.error("", e);
         }
         logger.info("[action=find][key=" + key + "][user=" + res + "][table="
                 + table + "][success=" + success + "]");
         return res;
     }
     
     
     public static boolean batchUpdate(List<Put> puts,String table, HConnection connection){
    	 if(puts.size()==0){
    		 return true;
    	 }
    	 
    	 StringBuffer sb = new StringBuffer();
    	 for(Put put:puts){
    		 sb.append(Bytes.toString(put.getRow())).append(",");
    	 }
    	 
    	 logger.info("---batch--update---" +puts.size() +"  --into table -" + table+"-----------"+sb.toString());
    	 boolean success = false;
    	 try {
    		 HTableInterface hTable = connection.getTable(table);
    		 Result[] results = new Result[puts.size()];
    		 hTable.batch(puts, results);
    		 success = true;
		} catch (Exception e) {
			logger.error("", e);
		}
    	 
    	return success; 
     }
     

    /**
     * update user id in Table "table" at key "key" with "user"
     * @param key: identity key, as row key seed
     * @param table: table name
     * @param user: user id to update
     * @return true if success, false if error
     */
    public static boolean update(String key, String table, String user, HConnection connection) {
        if (Strings.isNullOrEmpty(key)
                || Strings.isNullOrEmpty(table)
                || Strings.isNullOrEmpty(user)
                ) {
            logger.info("no mapping key or table name given, skip");
            return true;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[method=find],[key=" + key + "],[table="
                    + table + "],[user=" + user + "]");
        }

        byte[] rowKey = rowKeyGenerator.from(key, nRegions);
        Put put = new Put(rowKey);
        put.add(Family, Quantifier, Bytes.toBytes(user));

        boolean success = false;
        try {
//            HConnection connection = HBaseUtil.getConnection();
//            checkAndCreateTable(table, connection);
            HTableInterface hTable = connection.getTable(table);
            hTable.put(put);
            hTable.flushCommits();
            hTable.close();
            success = true;
//            HBaseUtil.releaseConnection(connection);
        } catch (IOException e) {
            // just skip
            logger.error("", e);
        }
        logger.info("[action=update][key=" + key + "][table="
                + table + "][user=" + user + "][success=" + success + "]");
        return success;
    }


    /**
     * find user id from Table chain "path" with key "key"
     * @param key: identity key, as row key seed
     * @param path: array of table name for looking up
     * @return user id with given key, or "" if not found
     */
    public static String findByProbing(String key, String[] path, HConnection connection, Integer index) {
        if (Strings.isNullOrEmpty(key)
                || null == path
                || path.length == 0) {
            logger.info("no mapping key or path info given, skip");
            return "";
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[method=find],[key=" + key + "],[path="
                    + Joiner.on(',').join(path) + "]");
        }

        String res = "";
//        HConnection connection = HBaseUtil.getConnection();
        for (int i = 0; i < path.length; ++i) {
            if (!Strings.isNullOrEmpty(res)) {
                index = i;
                break;
            }
            res = find(key, path[i], connection);
        }
//        HBaseUtil.releaseConnection(connection);

        return res;
    }


    /**
     * find user id from Table chain "path" with key "key"
     * @param key: identity key, as row key seed
     * @param path: array of table name for looking up
     * @return user id with given key, or "" if not found
     */
    public static String findByChain(String key, String[] path, HConnection connection, Integer index) {
        if (Strings.isNullOrEmpty(key)
                || null == path
                || path.length == 0) {
            logger.info("no mapping key or path info given, skip");
            return "";
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[method=find],[key=" + key + "],[path="
                    + Joiner.on(',').join(path) + "]");
        }

        String res = "";
//        HConnection connection = HBaseUtil.getConnection();
        for (int i = 0; i < path.length; ++i) {
            res = find(key, path[i], connection);
            if (Strings.isNullOrEmpty(res)) {
                index = i;
                break;
            }
            key = res;
        }
//        HBaseUtil.releaseConnection(connection);

        return res;
    }

    /**
     * create a new mapping table with name "table"
     * create 'dmp_xxx',
     * {NAME => 'common',COMPRESSION => 'SNAPPY'},
     * {MAX_FILESIZE => '1073741824',SPLITS_FILE => '/var/run/hbase/splits.txt'}
     * @param table: table name
     * @return true if success, false if error
     */
    private static boolean createTable(String table, HConnection connection) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("[method=createTable],[table=" + table
                                + "],[" + "MAX_FILESIZE=" + MAX_FILESIZE
                                + "],[" + "MAX_VERSIONS=" + MAX_VERSIONS
                                + "],[" + "COMPRESSION=" + COMPRESSION
                                + "],[" + "family=" + new String(Family)
                                + "]"
                );
            }

//            HConnection connection = HBaseUtil.getConnection();
            HBaseAdmin hBaseAdmin = new HBaseAdmin(connection);
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(table));
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Family);
            hColumnDescriptor.setCompressionType(COMPRESSION);
            hColumnDescriptor.setMaxVersions(MAX_VERSIONS);
            descriptor.addFamily(hColumnDescriptor);
            descriptor.setMaxFileSize(MAX_FILESIZE);
            int splits = 1;
            try {
                splits = Integer.parseInt(nRegions);
            } catch (NumberFormatException e) {

            }
            byte[][] preSplits = preSplit(splits);
            if (null == preSplits) {
                hBaseAdmin.createTable(descriptor);
            } else {
                hBaseAdmin.createTable(descriptor, preSplits);
            }
            hBaseAdmin.close();
//            HBaseUtil.releaseConnection(connection);
            return true;
        } catch (MasterNotRunningException e) {
            logger.error("", e);
        } catch (ZooKeeperConnectionException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }
        return false;
    }

    /**
     * check whether a mapping table exist
     * @param table: table name
     * @return true if exists, false if error or not exists
     */
    public static boolean isTableExists(String table, HConnection connection) {
        boolean exists = false;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("[method=createTable],[table=" + table + "]");
            }

//            HConnection connection = HBaseUtil.getConnection();
            HBaseAdmin hBaseAdmin = new HBaseAdmin(connection);
            exists = hBaseAdmin.tableExists(table);
            hBaseAdmin.close();
//            HBaseUtil.releaseConnection(connection);
        } catch (MasterNotRunningException e) {
            logger.error("", e);
        } catch (ZooKeeperConnectionException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }
        return exists;
    }

        /**
     * check whether a mapping table exist
     * @param table: table name
     * @return true if exists, false if error or not exists
     */
    public static boolean checkAndCreateTable(String table, HConnection connection) {
        boolean exists = false;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("[method=checkAndCreateTable],[table=" + table + "]");
            }

            HBaseAdmin hBaseAdmin = new HBaseAdmin(connection);
            exists = hBaseAdmin.tableExists(table);
            if (!exists) {
                HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(table));
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Family);
                hColumnDescriptor.setCompressionType(COMPRESSION);
                hColumnDescriptor.setMaxVersions(MAX_VERSIONS);
                hColumnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);// 逻辑压缩，提高blockcache使用率   PREFIX_TREE不但可以提高使用率还可以提高查询率  但是bug比较多，暂时使用FAST_DIFF
                hColumnDescriptor.setBlocksize(65536*2);
                hColumnDescriptor.setBloomFilterType(BloomType.ROW);// 设置bloomfilter  对随机读效果提升高   本需求都是随机读  没有scan
                descriptor.addFamily(hColumnDescriptor);
                descriptor.setMaxFileSize(MAX_FILESIZE);
                descriptor.setDurability(Durability.ASYNC_WAL);//异步写wal 在特许情况下会丢失一部分数据 ，但是不多
                int splits = 1;
                try {
                    splits = Integer.parseInt(nRegions);
                } catch (NumberFormatException e) {

                }
                byte[][] preSplits = preSplit(splits);
                if (null == preSplits) {
                    hBaseAdmin.createTable(descriptor);
                } else {
                    hBaseAdmin.createTable(descriptor, preSplits);
                }
                logger.info("[method=checkAndCreateTable],[table=" + table
                                + "],[" + "MAX_FILESIZE=" + MAX_FILESIZE
                                + "],[" + "MAX_VERSIONS=" + MAX_VERSIONS
                                + "],[" + "COMPRESSION=" + COMPRESSION
                                + "],[" + "family=" + new String(Family)
                                + "]"
                );
            }
            hBaseAdmin.close();
            exists = true;
        } catch (MasterNotRunningException e) {
            logger.error("", e);
        } catch (ZooKeeperConnectionException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }
        return exists;
    }


    /**
     * calculate split detail
     * @param min_id
     * @param max_id
     * @param chunks
     * @return
     */
    public static byte[][] getSplits(long min_id, long max_id, int chunks){
        long chunkSize = (max_id - min_id) / chunks;
        byte[][] splits = new byte[chunks+2][];
        int index = 0;
        for (Long i = min_id; i <= (max_id + chunkSize); i += chunkSize) {
            Long start = i;
            Long end = i + chunkSize - 1;
            splits[index] = (String.valueOf(end)).getBytes();
            index++;
        }

        return splits;
    }

    public static byte[][] preSplit(int nRegions) {
        if (nRegions <= 1) return null;
        int width = 1;
        int regions = nRegions;
        while (regions >= 10) {
            width++;
            regions /= 10;
        }
        byte[][] splits = new byte[nRegions][];
        for (int i = 1; i <= nRegions; ++i) {
            splits[i-1] = Strings.padStart("" + i, width, '0').getBytes();
            System.out.print(new String(splits[i-1]));
            System.out.print(",");
        }
        System.out.println("");

        return splits;
    }

}
