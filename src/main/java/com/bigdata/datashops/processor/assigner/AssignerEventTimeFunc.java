package com.bigdata.datashops.processor.assigner;

import java.io.Serializable;

public interface AssignerEventTimeFunc<T> extends Serializable {
    /**
     * 提取事件时间戳
     * @param t 数据记录
     * @return 事件时间 时间戳 精度到毫秒 长度13
     * @see System#currentTimeMillis()
     */
    public Long getEventTime(T t);
}
