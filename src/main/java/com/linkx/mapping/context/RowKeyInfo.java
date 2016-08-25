package com.linkx.mapping.context;

import com.linkx.mapping.function.row.RandomRowKey;
import com.linkx.mapping.function.row.RowKeyExtractionFn;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class RowKeyInfo {
    RowKeyExtractionFn fn = new RandomRowKey();
    
    boolean gobalChained = false;

    public RowKeyExtractionFn getFn() {
        return fn;
    }

    public void setFn(RowKeyExtractionFn fn) {
        this.fn = fn;
    }

	public boolean isGobalChained() {
		return gobalChained;
	}

	public void setGobalChained(boolean gobalChained) {
		this.gobalChained = gobalChained;
	}
    
    
}
