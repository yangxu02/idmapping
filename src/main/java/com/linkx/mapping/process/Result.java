package com.linkx.mapping.process;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class Result {
    boolean success;
    UserLinkage linkage;
    
    List<Row> puts = new ArrayList<Row>();
    String table = "";

    Result(boolean success) {
        this.success = success;
    }

    public Result(boolean success, UserLinkage linkage) {
        this.success = success;
        this.linkage = linkage;
    }
    
    public void addRow(Row put){
    	puts.add(put);
    }
    
    public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void addRows( List<Row> puts){
    	puts.addAll(puts);
    }
    public void addPuts( List<Put> puts){
    	puts.addAll(puts);
    }

  

	public List<Row> getPuts() {
		return puts;
	}

	public void setPuts(List<Row> puts) {
		this.puts = puts;
	}



	final static Result ok = new Result(true);

    public UserLinkage getLinkage() {
        return linkage;
    }
}
