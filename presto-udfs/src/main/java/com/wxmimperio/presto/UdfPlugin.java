package com.wxmimperio.presto;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wxmimperio on 2018/2/27.
 */
public class UdfPlugin implements Plugin {

    @Override
    public <T> List<T> getServices(Class<T> aClass) {
        return null;
    }
}
