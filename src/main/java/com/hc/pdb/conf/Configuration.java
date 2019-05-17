package com.hc.pdb.conf;

import java.util.HashMap;
import java.util.Map;

public class Configuration {
    private Map<String, Object> k2v = new HashMap<>();

    public void put(String key, Object value) {
        k2v.put(key, value);
    }

    public Long getLong(String key, long defaultValue) {
        Object o = k2v.get(key);

        if (o != null && o instanceof String) {
            return Long.parseLong((String) o);
        }

        return defaultValue;
    }

    public Integer getInt(String key, int defaultValue) {
        Object o = k2v.get(key);

        if (o != null && o instanceof String) {
            return Integer.parseInt((String) o);
        }

        return defaultValue;
    }

    public Double getDouble(String key, double defaultValue) {
        Object o = k2v.get(key);

        if (o != null && o instanceof String) {
            return Double.parseDouble((String) o);
        }

        return defaultValue;
    }

    public String get(String key) {
        return get(key, null);
    }

    public String get(String key, String defaultValue) {
        Object o = k2v.get(key);

        if (o == null) {
            return defaultValue;
        }

        if (o instanceof String) {
            return (String) o;
        }

        throw new ConfValueCastException(o.getClass().getName() + " can not cast to String");
    }

}
