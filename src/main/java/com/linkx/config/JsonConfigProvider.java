package com.linkx.config;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Provider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/16.
 */
public class JsonConfigProvider<T> implements Provider<T> {

    final Class<T> clazz;

    public JsonConfigProvider(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T get() {

        String cfgFile = "runtime.properties";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream stream = classLoader.getResourceAsStream(cfgFile);
        if (null == stream) {
            cfgFile = System.getProperty("app.runtime");
            Preconditions.checkArgument(Strings.isNullOrEmpty(cfgFile), "no runtime config file found:" + cfgFile);
            stream = classLoader.getResourceAsStream(cfgFile);
        }
        Log logger = LogFactory.getLog(this.getClass());
        T cfg = null;
        try {
            /*
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            String line;
            while (null != (line = reader.readLine())) {
                System.out.println(line);
            }
            */
            cfg = new ObjectMapper().readValue(stream, clazz);
        } catch (JsonMappingException e) {
            logger.error("", e);
        } catch (JsonParseException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }

        Preconditions.checkNotNull(cfg, "Config loading error with file:" + cfgFile);

        return cfg;
    }
}
