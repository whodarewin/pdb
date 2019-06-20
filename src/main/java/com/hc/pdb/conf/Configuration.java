package com.hc.pdb.conf;

import java.util.HashMap;
import java.util.Map;


/**
 * Configuration
 * 整个的配置
 * @author han.congcong
 * @date 2019/6/3
 */

public class Configuration {
    private Map<String, Object> k2v = new HashMap<>();

    public void put(String key, Object value) {
        k2v.put(key, value);
    }

    public Long getLong(String key, long defaultValue) {
        Object o = k2v.get(key);

        if (o instanceof String) {
            return Long.parseLong((String) o);
        }

        if(o instanceof Long){
            return (Long) o;
        }

        if(o == null){
            return defaultValue;
        }

        throw new WrongClassException("expected : long", null);
    }

    public Integer getInt(String key, int defaultValue) {
        Object o = k2v.get(key);

        if (o instanceof String) {
            return Integer.parseInt((String) o);
        }

        if(o instanceof Integer){
            return (Integer) o;
        }

        if(o == null){
            return defaultValue;
        }

        throw new WrongClassException("expected : int", null);
    }

    public Double getDouble(String key, double defaultValue) {
        Object o = k2v.get(key);

        if (o instanceof String) {
            return Double.parseDouble((String) o);
        }

        if(o instanceof Integer){
            return (Double) o;
        }

        if(o == null){
            return defaultValue;
        }

        throw new WrongClassException("expected : double", null);
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
