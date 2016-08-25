package com.linkx.mapping.feed;
/**
 * Created by yangxu on 12/16/14.
 */

public interface Feed {
    boolean open();

    String next();

    boolean close();
}
