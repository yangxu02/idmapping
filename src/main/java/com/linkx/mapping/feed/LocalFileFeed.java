package com.linkx.mapping.feed;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * Created by yangxu on 12/16/14.
 * only accept file
 */


public class LocalFileFeed implements Feed {
    private final static Log logger = LogFactory.getLog(LocalFileFeed.class);

    final String path;
    InputStream stream = null;
    BufferedReader reader = null;

    public LocalFileFeed(String path) {
        this.path = path;
    }

    @Override
    public boolean open() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "no hdfs path give");
        try {

            stream = new FileInputStream(path);

            reader = new BufferedReader(new InputStreamReader(stream, Charsets.UTF_8));

            return true;
        } catch (IOException e) {
            logger.error("", e);
        }
        return false;

    }

    @Override
    public String next() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            logger.error("", e);
        }
        return "";
    }

    @Override
    public boolean close() {
        if (null != reader) {
            try {
                reader.close();
                stream.close();
            } catch (IOException e) {
                logger.error("", e);
                return false;
            }
        }
        return true;
    }

}
