package com.wxmimperio.kudu.utils;

import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

public class ResourceUtils {
    private static ResourceBundle resource;

    static {
        resource = ResourceBundle.getBundle("application");
    }

    public static List<String> getByList(String key) {
        return Arrays.asList(resource.getString(key).split(",", -1));
    }

    public static String getByString(String key) {
        return resource.getString(key);
    }
}
