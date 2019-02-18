package com.hc.pdb.hcc;
/**
 * 数据结构
 * prefix : hcc的utf-8编码的byte数组
 * data : 数据区域，最小单位为block，包含多个cell
 * index: 索引区域。key的长度(bit 表示) + key
 * bloomFilter:直接写入的不拢过滤器
 * meta info：index start + bloom start
 */