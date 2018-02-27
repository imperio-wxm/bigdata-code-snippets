package com.wxmimperio.phoenix.utils;

import java.util.ResourceBundle;

public class PropertiesUtils {

    private ResourceBundle bundle = null;

    public PropertiesUtils(String propertiesFile) {
        initBundle(propertiesFile);
    }

    private void initBundle(String propertiesFile) {
        this.bundle = ResourceBundle.getBundle(propertiesFile);
    }

    public String getString(String key) {
        return this.bundle.getString(key);
    }

    public String getPhoenixConnectUrl() {
        return this.bundle.getString("phoenix.jdbc.connect.url");
    }

    public int getInt(String key) {
        return Integer.parseInt(this.bundle.getString(key));
    }
}
