package com.hc.pdb.conf;

import org.apache.commons.beanutils.ConvertUtils;

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

    public long getLong(String key){
        return getLong(key,-1);
    }

    public long getLong(String key, long defaultValue) {
        Object o = k2v.get(key);
        if(o == null){
            return defaultValue;
        }

    return (long) ConvertUtils.convert(o,Long.class);
    }

    public int getInt(String key){
        return getInt(key,-1);
    }

    public int getInt(String key, int defaultValue) {
        Object o = k2v.get(key);

        if(o == null){
            return defaultValue;
        }

        return (int) ConvertUtils.convert(o,Integer.class);
    }

    public double getDouble(String key){
        return getDouble(key,-1);
    }

    public double getDouble(String key, double defaultValue) {
        Object o = k2v.get(key);
        if(o == null){
            return defaultValue;
        }

        return (double) ConvertUtils.convert(o,Double.class);
    }

    public String get(String key) {
        return get(key, null);
    }

    public String get(String key, String defaultValue) {
        Object o = k2v.get(key);

        if (o == null) {
            return defaultValue;
        }

        return (String) ConvertUtils.convert(o,String.class);
    }

}
