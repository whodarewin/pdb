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

    public Long getLong(String key, long defaultValue) {
        Object o = k2v.get(key);
        if(o == null){
            return defaultValue;
        }

        return (Long) ConvertUtils.convert(o,Long.class);
    }

    public Integer getInt(String key, int defaultValue) {
        Object o = k2v.get(key);

        if(o == null){
            return defaultValue;
        }

        return (Integer) ConvertUtils.convert(o,Integer.class);
    }

    public Double getDouble(String key, double defaultValue) {
        Object o = k2v.get(key);
        if(o == null){
            return defaultValue;
        }

        return (Double) ConvertUtils.convert(o,Double.class);
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
