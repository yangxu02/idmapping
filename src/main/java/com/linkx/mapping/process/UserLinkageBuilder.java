package com.linkx.mapping.process;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.linkx.mapping.table.hbase.HBaseMappingTables;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.DimensionInfo;
import com.linkx.mapping.context.IdentityInfo;
import com.linkx.mapping.context.TableInfo;
import com.linkx.mapping.row.Row;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 * checkout identity -> user relations
 * 1: {a->u} -> {a-u}
 * 2: {i->b, b->c, c->u} -> {i-u}
 */
public class UserLinkageBuilder extends Worker {

    private final static Log logger = LogFactory.getLog(UserLinkageBuilder.class);
    protected String name = UserLinkageBuilder.class.getSimpleName();

    @Override
    public Result doWork(final Row row, Result result) {
        Context context = ctx.get(row);
        if (null == context
                || null == row
                || test) return Result.ok;
        HConnection connection = ctx.gethConnection();

        tickerStart();

        Preconditions.checkNotNull(row, "no input row");
        Preconditions.checkNotNull(ctx, "context must be set before processing");
        Preconditions.checkNotNull(context.getMapping(), "mapping context must be set before processing");
        IdentityInfo[] infos = context.getMapping().getIdentities();
        Preconditions.checkArgument(null != infos && 0 != infos.length, "at least one identity needed");
       /* logger.info("--------"+infos.length );
        for(IdentityInfo identityInfo:infos){
        	logger.info("---->>"+identityInfo.getLookup().length);
        }*/

        logDuration(logger, "[task=PreconditionCheck]");

        List<Linkage> linkages = Lists.newArrayList();
        
        boolean chaind = context.getMapping().getUserId().isGobalChained();
        logger.info("-----------------"+chaind+"-------------------------------------");
        if(chaind){
        	for (int i = 0; i < infos.length; ++i) {
        		IdentityInfo info = infos[i];
        		String id = getIdentity(row, info);
        		if (Strings.isNullOrEmpty(id)) continue;
        		
        		Linkage linkage = getProvidedLinkage(id, info, connection,"|"+info.getName());
        		
        		logDuration(logger, "[task=getProvidedLinkage]"
        				+ "][info.ident=" + id
        				+ "][info.name=" + info.getName()
        				+ "]"
        				);
        		
        		// skipNulls
        		if (info.isSkipNulls() && Strings.isNullOrEmpty(linkage.getUserId())) {
        			continue;
        		}
        		addLinkage(linkage, info, linkages);
        	}
        }else {
        	getPageLinkByBatch(row,infos,connection,linkages); //scott.zhai 20150402
        }
        

        UserLinkage userLinkage = checkAndReassignLinkage(row, linkages, context);
        

        logDuration(logger, "[task=checkAndReassignLinkage]");

        tickerReset();

        boolean ok = (null != userLinkage);
        return new Result(ok, userLinkage);
    }

    private void getPageLinkByBatch( Row row,IdentityInfo[] infos, HConnection connection,List<Linkage> linkages) {
    	if(infos==null||infos.length==0){
    		return ;
    	}
    	String table = "";
    	int length = 0;
    	List<Get> gets = new ArrayList<Get>();
    	List<IdentityInfo> identityInfos = new ArrayList<IdentityInfo>();
    	List<String> ids = new ArrayList<String>();
    	for(IdentityInfo identityInfo:infos){
    		TableInfo[] lookUps = identityInfo.getLookup();
    		if(lookUps==null|| lookUps.length==0){
    			continue;
    		}
    		//logger.info(" for lookUps length--->>" +lookUps.length );
    		String id = getIdentity(row, identityInfo);
    		//logger.info(" id------->>" + id);
            if (Strings.isNullOrEmpty(id)) continue;
    		table = lookUps[0].getTable();
    		String curId = "";
    		Get get = null;
    		for(int i = 0; i < lookUps.length ; ++i){
    			length++;
    			TableInfo lookup = lookUps[i];
    			curId = lookup.getHasher().hash(id);
    			get = new Get(Bytes.toBytes(curId+"|"+identityInfo.getName()));
    			gets.add(get);
    			identityInfos.add(identityInfo);
    			ids.add(id);
    		}
    	}
    	if("".equals(table)){
    		return;
    	}
    	org.apache.hadoop.hbase.client.Result[] results = new org.apache.hadoop.hbase.client.Result[length];
    	
		
		HBaseMappingTables.batchFind(gets, table, connection, results);
		
		logger.info("result length is " + results.length);
		
		handlerBatchResult(gets,results,linkages,table,identityInfos,ids);
	}

	private void handlerBatchResult(List<Get> gets,org.apache.hadoop.hbase.client.Result[] results,List<Linkage> linkages,String table,List<IdentityInfo> identityInfos,List<String> ids) {
		
		Linkage linkage = null;
		int length = results.length;
		
		for(int i=0;i<length;i++){
			org.apache.hadoop.hbase.client.Result result = results[i];
			if(result.getRow()==null){
				linkage = new Linkage(ids.get(i), "", "",identityInfos.get(i).getName());
			}else {
				linkage = new Linkage(ids.get(i), table, Bytes.toString(result.getValue(HBaseMappingTables.Family, HBaseMappingTables.Quantifier)),identityInfos.get(i).getName());
			}
			
			if (identityInfos.get(i).isSkipNulls() &&  Strings.isNullOrEmpty(linkage.getUserId())) {
                continue;
            }
			addLinkage(linkage, identityInfos.get(i), linkages);
		}
	}

	private Linkage getProvidedLinkage(String id, IdentityInfo info, HConnection connection,String rowKeySuffix) {
        if (null == info || null == info.getLookup()
                || 0 == info.getLookup().length
                || Strings.isNullOrEmpty(id)) return null;

        if (!info.isChained()) {
            return getProvidedLinkageByProbe(id, info, connection,rowKeySuffix);
        }
        return getProvidedLinkageByChain(id, info, connection,rowKeySuffix);
    }

    private List<Linkage> getProvidedLinkages(String id, IdentityInfo info, HConnection connection) {
        if (null == info || null == info.getLookup()
                || 0 == info.getLookup().length
                || Strings.isNullOrEmpty(id)) return null;

        if (!info.isChained()) {
            return getProvidedLinkagesByProbe(id, info, connection);
        }
        Linkage linkage = getProvidedLinkageByChain(id, info, connection,null);
        return null == linkage ? Collections.EMPTY_LIST : Lists.newArrayList(linkage);
    }


    private List<Linkage> getProvidedLinkagesByProbe(String id, IdentityInfo info, HConnection connection) {
        TableInfo[] lookUps = info.getLookup();
        String user = "";
        String table = "";
        String curId = "";
        List<Linkage> linkages = null;
        for (int i = 0; i < lookUps.length && Strings.isNullOrEmpty(user); ++i) {
            TableInfo lookup = lookUps[i];
            table = lookUps[i].getTable();
            curId = lookup.getHasher().hash(id);
            user = HBaseMappingTables.find(curId, table, connection);
            if (null == linkages) {
                linkages = new ArrayList<>();
            }
            linkages.add(new Linkage(curId, table, user));
        }
        return linkages;
    }
    
    private Linkage getProvidedLinkageByProbe(String id, IdentityInfo info, HConnection connection,String rowKeySuffix) {
        TableInfo[] lookUps = info.getLookup();
        String user = "";
        String table = "";
        for (int i = 0; i < lookUps.length && Strings.isNullOrEmpty(user); ++i) {
            TableInfo lookup = lookUps[i];
            table = lookUps[i].getTable();
            user = HBaseMappingTables.findWhithSuffix(lookup.getHasher().hash(id), table, connection,rowKeySuffix,true);
        }
        if (Strings.isNullOrEmpty(user)) table = "";
        return new Linkage(id, table, user,info.getName());
    }

    private Linkage getProvidedLinkageByChain(String id, IdentityInfo info, HConnection connection,String rowKeySuffix) {
        TableInfo[] lookUps = info.getLookup();
        String table = "";
        String user = lookUps[0].getHasher().hash(id);
        int i = 0;
        for (; i < lookUps.length && !Strings.isNullOrEmpty(user); ++i) {
            table = lookUps[i].getTable();
            id = user;
            logger.info("--------"+id+"------------"+table);
            if(i==0){
            	user = HBaseMappingTables.findWhithSuffix(id, table, connection,rowKeySuffix,false);
            }else {
            	user = HBaseMappingTables.findWhithSuffix(id, table, connection,rowKeySuffix,true);
            }
        }
        if (i < lookUps.length) return null;
        return new Linkage(id, table, user,info.getName());
    }



    /**
     * get identity value from input row according give identity context
     * @param row: row to process
     * @param info: identity info
     * @return: final identity value
     */
    private String getIdentity(Row row, IdentityInfo info) {
        if (null == row
                || null == info) {
            return "";
        }

        // which rows to derive identity from
        DimensionInfo[] dims = info.getDims();
        if (null == dims
                || 0 == dims.length) {
            return "";
        }
        String[] values = new String[dims.length];
        for (int i = 0; i < dims.length; ++i) {
            values[i] = dims[i].getFn().apply(row, dims[i].getName());
        }

        return info.getJoiner().apply(values);
    }

      /**
     * build identity->user linkage relation according configuration
     * @param linkage: linkage info
     * @param info : identity info, contains which tables to save linkage
     * @param linkages: linkage list
     */
    private void addLinkage(Linkage linkage,
                            IdentityInfo info,
                            List<Linkage> linkages
                            ) {
        if (null == linkage
                || null == info) {
            return;
        }
        TableInfo tableInfo;
        String userId = linkage.getUserId();
        String tableName = linkage.getTable();
        String identity = linkage.getIdentity();
        String oldTbaleName = linkage.getOldTbaleName();

        TableInfo[] writeback = info.getWriteback();
        if (null != writeback && 0 != writeback.length) {
            for (int i = 0; i < writeback.length; ++i) {
                tableInfo = writeback[i];
                if (null == tableInfo
                        || Strings.isNullOrEmpty(tableInfo.getTable())
                        /*
                        || (
                            tableName.equalsIgnoreCase(tableInfo.getTable())
                            && !Strings.isNullOrEmpty(userId) // id->user already exists
                            )
                            */
                        ) {
                    // skip
                    continue;
                }
                linkages.add(new Linkage(tableInfo.getHasher().hash(identity),
                        tableInfo.getTable(), userId,oldTbaleName));
            }
        }

        if (logger.isDebugEnabled()) {
            logger.info(new Gson().toJson(linkages));
        }
    }


    /**
     * build identity->user linkage relation according configuration
     * @param identity: identity value
     * @param userId: user id
     * @param info : identity info, contains which tables to save linkage
     * @param linkages: linkage list
     */
    private void addLinkage(String identity,
                            String userId,
                            IdentityInfo info,
                            int tableIndex,
                            List<Linkage> linkages
                            ) {
        if (Strings.isNullOrEmpty(identity)
                || null == info) {
            return;
        }

        String tableName = "";
        if (null != info.getLookup()
                && tableIndex >= 0
                && tableIndex < info.getLookup().length) {
            tableName = info.getLookup()[tableIndex].getTable();
        }

        TableInfo tableInfo;

        TableInfo[] writeback = info.getWriteback();
        if (null != writeback && 0 != writeback.length) {
            for (int i = 0; i < writeback.length; ++i) {
                tableInfo = writeback[i];
                if (null == tableInfo
                        || Strings.isNullOrEmpty(tableInfo.getTable())
                        || tableName.equalsIgnoreCase(tableInfo.getTable())) {
                    // skip
                    continue;
                }
                linkages.add(new Linkage(tableInfo.getHasher().hash(identity),
                        tableInfo.getTable(), userId));
            }
        }
        /*
        else if (null != info.getLookup() && 0 != info.getLookup().length) {
            // 避免漏配writeback
            tableInfo = info.getLookup()[0];
            tableName = tableInfo.getTable();
            linkages.add(new Linkage(tableInfo.getHasher().hash(identity),
                    tableName, userId));
        }
        */

        if (logger.isDebugEnabled()) {
            logger.info(new Gson().toJson(linkages));
        }
    }


    private String getUserId(String id, TableInfo[] lookups, boolean chained, Integer index, HConnection conn) {
        String[] path = new String[lookups.length];
        for (int i = 0; i < lookups.length; ++i) {
            path[i] = lookups[i].getTable();
        }
        if (chained) {
            return HBaseMappingTables.findByChain(id, path, conn, index);
        } else {
            return HBaseMappingTables.findByProbing(id, path, conn, index);
        }
    }

    private String getUserIdHasherEnabled(String id, TableInfo[] lookups, boolean chained, Integer index, HConnection conn) {
        if (chained) {
            String[] path = new String[lookups.length];
            for (int i = 0; i < lookups.length; ++i) {
                path[i] = lookups[i].getTable();
            }
            return HBaseMappingTables.findByChain(lookups[0].getHasher().hash(id), path, conn, index);
        } else {

            String user = "";
            for (int i = 0; i < lookups.length && Strings.isNullOrEmpty(user); ++i) {
                TableInfo lookup = lookups[i];
                user = HBaseMappingTables.find(lookup.getHasher().hash(id), lookup.getTable(), conn);
                index = i;
            }
            return user;
        }
    }


    /**
     * check if user exists or not:
     * 1. if not, assign a new user id from data input or auto-generation
     * 2. if only one, use this one
     * 3. if multiple, use the first user found
     * @param linkages: identity->user linkages
     * @return user linkage for update
     */
    private UserLinkage checkAndReassignLinkage(Row row, List<Linkage> linkages, Context context) {
        if (null == linkages
                || linkages.isEmpty()) return null;
        String userid = "";
        for (int i = 0; i < linkages.size(); ++i) {
            Linkage linkage = linkages.get(i);
            if (!Strings.isNullOrEmpty(linkage.getUserId())) {
                // first no empty
                userid = linkage.getUserId();
                break;
            }
        }
        if (Strings.isNullOrEmpty(userid)) {
            userid = context.getMapping().getUserId().getFn().apply(row);
//            userid = UUID.randomUUID().toString();
        }
        logger.info(" final userid is ------------>" +userid);
        return new UserLinkage(userid, linkages);
    }
}

