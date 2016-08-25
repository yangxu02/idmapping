package com.linkx.mapping.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.linkx.mapping.context.Context;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class ContextModule extends AbstractModule {
    @Override
    protected void configure() {
        String type = System.getProperty("dmp.mapping.context.single", "false");
        if ("false".equalsIgnoreCase(type)) {
            bind(Context.class).toProvider(MultipleContextProvider.class).in(Singleton.class);
        } else {
            bind(Context.class).toProvider(ContextProvider.class).in(Singleton.class);
        }
    }
}
