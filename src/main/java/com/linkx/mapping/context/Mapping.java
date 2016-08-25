package com.linkx.mapping.context;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/10.
 */
public class Mapping {
    // all of the identities info of input
    IdentityInfo[] identities;

    // user id generation function
    RowKeyInfo userId = new RowKeyInfo();

    public IdentityInfo[] getIdentities() {
        return identities;
    }

    public void setIdentities(IdentityInfo[] identities) {
        this.identities = identities;
    }

    public RowKeyInfo getUserId() {
        return userId;
    }

    public void setUserId(RowKeyInfo userId) {
        this.userId = userId;
    }
}
