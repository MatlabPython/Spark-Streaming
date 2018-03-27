package com.gsafety.lifeline.bigdata.znodeWatch;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hadoop on 2017/9/4.
 */
public class SystemConfig {
    private static Properties props;

    static {
        props = new Properties();
        try {
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
            props.load(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }

    public static String get(String key, String defaultValue) {
        String v = props.getProperty(key);
        if (v != null && !"".equals(v)) {
            return v;
        } else {
            return defaultValue;
        }
    }

    public static int getInt(String key) {
        String value = props.getProperty(key);
        return Integer.parseInt(value);
    }
}
