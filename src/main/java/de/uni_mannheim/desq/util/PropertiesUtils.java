package de.uni_mannheim.desq.util;

import java.util.Properties;

/**
 * Created by rgemulla on 18.07.2016.
 */
public class PropertiesUtils {
    /**
     *
     * @param key
     * @return
     * @throws IllegalArgumentException if property does not exist
     */
    public static String get(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("property " + key + " not found");
        }
        return value;
    }

    public static boolean getBoolean(Properties properties, String key) {
        return Boolean.parseBoolean(get(properties, key));
    }

    public static int getInt(Properties properties, String key) {
        return Integer.parseInt(get(properties, key));
    }

    public static long getLong(Properties properties, String key) {
        return Long.parseLong(get(properties, key));
    }

    public static void set(Properties properties, String key, boolean value) {
        properties.setProperty(key, Boolean.toString(value));
    }

    public static void set(Properties properties, String key, int value) {
        properties.setProperty(key, Integer.toString(value));
    }

    public static void set(Properties properties, String key, long value) {
        properties.setProperty(key, Long.toString(value));
    }
    
    public static void set(Properties properties, String key, String value) {
    	properties.setProperty(key, value);
    }
}
