package com.linkx.mapping;

import com.google.common.base.Preconditions;
import com.linkx.mapping.feed.HdfsFileFeed;
import com.linkx.mapping.feed.LocalFileFeed;
import com.linkx.mapping.process.Mapper;
import com.linkx.mapping.feed.Feed;
import com.linkx.hbase.HBaseUtil;

import java.io.IOException;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class App {
    public static void main(String[] args) throws IOException {
        if (null == args || 2 != args.length) {
            System.out.println("no input file given");
            return;
        }
        String fs = args[0];
        String file = args[1];
        Feed feed = null;
        if ("local".equalsIgnoreCase(fs)) {
            feed = new LocalFileFeed(file);
        } else if ("hdfs".equalsIgnoreCase(fs)) {
            feed = new HdfsFileFeed(file, HBaseUtil.getConf());
        }

        if (null != feed) {
            Preconditions.checkState(feed.open(), "open " + feed.getClass().getSimpleName() + " failed at path " + file);
            Mapper mapper = new Mapper().setup();
            String line;
            while (null != (line = feed.next())) {
                mapper.start(line);
            }
            mapper.cleanup();
            feed.close();
        } else {
            System.out.println("unsupport file system:" + fs);
        }
    }
}
