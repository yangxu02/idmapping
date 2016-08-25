package com.linkx.mapping.process;

import com.google.common.base.Objects;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class Linkage {
    // identity field in input
    String identity;
    // identity's mapping table
    String table;
    // identity's user id
    String userId;
    
    String oldTbaleName; 

    public Linkage(String identity, String table, String userId,
			String oldTbaleName) {
		super();
		this.identity = identity;
		this.table = table;
		this.userId = userId;
		this.oldTbaleName = oldTbaleName;
	}

	public Linkage(String identity, String table, String userId) {
        this.identity = identity;
        this.table = table;
        this.userId = userId;
    }

    public String getOldTbaleName() {
		return oldTbaleName;
	}

	public void setOldTbaleName(String oldTbaleName) {
		this.oldTbaleName = oldTbaleName;
	}

	public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("identity", identity)
                .add("table", table)
                .add("userId", userId)
                .toString();
    }
}
