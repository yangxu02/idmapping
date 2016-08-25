package com.linkx.mapping;

import com.linkx.hbase.RowKeyGenerator;

import java.io.IOException;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class KeyIndex {
    public static void main(String[] args) throws IOException {
        if (null == args || 2 <= args.length) {
            System.out.println("not enough param given");
        }
        String key = args[0];
        String regions = args[1];

        String finalKey = new String(RowKeyGenerator.hash.from(key, regions));

        System.out.println(key + "+" + regions + "=" + finalKey);

    }
}
