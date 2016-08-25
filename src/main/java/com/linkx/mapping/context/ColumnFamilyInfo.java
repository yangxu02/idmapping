package com.linkx.mapping.context;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/18.
 */
public class ColumnFamilyInfo {

    // column family name
    String family;

    // columns in this family
    DimensionInfo[] cols;

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public DimensionInfo[] getCols() {
        return cols;
    }

    public void setCols(DimensionInfo[] cols) {
        this.cols = cols;
    }
}
