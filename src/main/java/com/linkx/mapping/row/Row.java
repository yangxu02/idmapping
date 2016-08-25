package com.linkx.mapping.row;

import java.util.Collection;
import java.util.List;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/10.
 */
public abstract class Row {

    public abstract String getDimensionVal(String dim);

    public abstract long getLongVal(String dim);

    public abstract Collection<String> getValues();

    public abstract List<String> getValues(String[] dims);

    public abstract Object getData();
}
