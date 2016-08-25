package com.linkx.mapping.feed;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by yangxu on 12/16/14.
 * only accept file
 */


public class HdfsFileFeed implements Feed {
    private final static Log logger = LogFactory.getLog(HdfsFileFeed.class);

    final String path;
    final Configuration conf;
    InputStream stream = null;
    BufferedReader reader = null;

    public HdfsFileFeed(String path, Configuration conf) {
        this.path = path;
        this.conf = conf;
    }

    @Override
    public boolean open() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "no hdfs path give");
        Preconditions.checkNotNull(conf, "no hdfs conf");
        Path location = new Path(path);
        try {
            FileSystem fileSystem = FileSystem.get(location.toUri(), conf);

            Preconditions.checkState(!fileSystem.exists(location), "not found:" + path);
            Preconditions.checkState(!fileSystem.isFile(location), "only file supported, not dir:" + path);

            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            CompressionCodec codec = factory.getCodec(location);
             // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(location));
            } else {
                stream = fileSystem.open(location);
            }

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
