package com.linkx.mapping.table;

import com.linkx.mapping.payload.PayLoad;
import com.linkx.mapping.process.UserLinkage;
import org.apache.hadoop.hbase.client.HConnection;

import java.util.Map;
import java.util.Set;

/**
 * Created by yangxu on 12/16/14.
 */
public interface UserTable {

    public boolean updateAliases(UserLinkage userLinkage, HConnection connection);

    public boolean updateDevices(String user, PayLoad payLoad, HConnection connection);

    public boolean updateEventCounters(String user, PayLoad payLoad, HConnection connection);

    public boolean updateProfile(String user, PayLoad payLoad, HConnection connection);

    public boolean getDevices(String user, HConnection connection, Map<String, String> devices, Set<String> alisas);
}
