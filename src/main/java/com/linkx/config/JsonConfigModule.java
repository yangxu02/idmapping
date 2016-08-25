package com.linkx.config;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/12.
 */
public class JsonConfigModule<T> extends AbstractModule {

    final Class<T> clazz;

    final Provider provider;

    public JsonConfigModule(Class clazz,
                            Provider provider) {
        this.clazz = clazz;
        this.provider = provider;
    }

    @Override
    protected void configure() {
        bind(clazz).toProvider(provider).in(Singleton.class);
    }
}
