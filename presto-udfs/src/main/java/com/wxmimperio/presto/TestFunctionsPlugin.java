package com.wxmimperio.presto;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Created by wxmimperio on 2018/2/19.
 */
public class TestFunctionsPlugin implements Plugin {

    @Override
    public <T> List<T> getServices(Class<T> aClass) {
        return ImmutableList.of();
    }
}
