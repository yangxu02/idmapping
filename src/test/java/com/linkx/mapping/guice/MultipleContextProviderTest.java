package com.linkx.mapping.guice;

import com.google.gson.GsonBuilder;
import com.linkx.mapping.context.Context;
import com.linkx.mapping.guice.MultipleContextProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * MultipleContextProvider Tester.
 *
 * @author <YangXu>
 * @since <pre>Jan 20, 2015</pre>
 * @version 1.0
 */
public class MultipleContextProviderTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: get()
     *
     */
    @Test
    public void testGet() throws Exception {
        Properties prop = System.getProperties();
        String cfgFile = System.setProperty("dmp.mapping.conf", "mapping.conf.v2");
        Context context = new MultipleContextProvider().get();

        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(context));
    }

    /**
     *
     * Method: parseInput(BufferedReader reader, ClassLoader loader, ObjectMapper mapper)
     *
     */
    @Test
    public void testParseInput() throws Exception {
//TODO: Test goes here... 
    }

    /**
     *
     * Method: parseContext(BufferedReader reader, ClassLoader loader, ObjectMapper mapper, Input input, Map<Expression, Context> contextMap)
     *
     */
    @Test
    public void testParseContext() throws Exception {
//TODO: Test goes here... 
    }


} 
