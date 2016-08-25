package com.linkx.mapping.payload;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.context.DimensionInfo;
import com.linkx.mapping.context.TableOutput;
import com.linkx.mapping.process.Result;
import com.linkx.mapping.row.Row;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class ProfilePayLoadBuilder implements PayLoadBuilder {

    private final static Log logger = LogFactory.getLog(ProfilePayLoadBuilder.class);

    /**
     * build user profile mapping
     * if age is "23", country is "cn", profile mapping will
     * be add with two entry key="age", value=23 and key="country", value="cn"
     * @param row: input row contains all values and dimension names
     * @param ctx: mapping configuration context
     * @return device info to update
     */
    @Inject
    public PayLoad build(Row row, Result result, Context ctx) {

        TableOutput output = ctx.getOutput().getProfiles();
        if (null == output) return null;
        // set columns
        DimensionInfo[] infos = output.getColumnFamily().getCols();
        if (null == infos || 0 == infos.length) return null;

        PayLoad payLoad = new PayLoad();
        // set table
        payLoad.setTable(output.getTable());
        // set column family
        payLoad.setColumnFamily(output.getColumnFamily().getFamily());
        // set row key
        payLoad.setRowKey(result.getLinkage().getUserId());

        for (int i = 0; i < infos.length; ++i) {
            DimensionInfo info =infos[i];
            String val = info.getFn().apply(row.getDimensionVal(info.getName()));
            if (Strings.isNullOrEmpty(val)) continue;
            if ("long".equalsIgnoreCase(info.getValType())) {
                long lval = 0;
                try {
                    lval = Long.parseLong(val);
                } catch (Exception e) {

                }
                payLoad.data.put(info.getColumn(), lval);
            } else {
                payLoad.data.put(info.getColumn(), val);
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[type=device],[payload=" + new Gson().toJson(payLoad.getData()) + "]");
        }

        return payLoad;
    }

}
