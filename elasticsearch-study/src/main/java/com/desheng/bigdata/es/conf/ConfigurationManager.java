package com.desheng.bigdata.es.conf;

import java.io.IOException;
import java.util.Properties;

public class ConfigurationManager {

    private static Properties properties;
    static {
        properties = new Properties();
        try {
            properties.load(ConfigurationManager.class.getClassLoader().getResourceAsStream("elasticsearch.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getProperties(String key) {
        return properties.getProperty(key);
    }

}
