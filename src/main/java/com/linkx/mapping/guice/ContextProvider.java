package com.linkx.mapping.guice;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Provider;
import com.linkx.mapping.context.Context;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class ContextProvider implements Provider<Context> {

    @Override
    public Context get() {
        String cfgFile = System.getProperty("dmp.mapping.conf", "mapping.rules");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream stream = classLoader.getResourceAsStream(cfgFile);
        Log logger = LogFactory.getLog(ContextProvider.class);
        Context context = null;
        try {
            /*
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            String line;
            while (null != (line = reader.readLine())) {
                System.out.println(line);
            }
            */
            context = new ObjectMapper().readValue(stream, Context.class);
        } catch (JsonMappingException e) {
            logger.error("", e);
        } catch (JsonParseException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }

        Preconditions.checkNotNull(context, "Mapping Context loading error with file:" + cfgFile);

        LogFactory.getLog(ContextProvider.class).info("successfully loading Mapping Context from file:" + cfgFile);

        return context;
    }

    private void test(){}
}
