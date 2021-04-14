package com.bigdata.datashops.processor.assigner;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class CommonEventTimeBucketAssigner<T> implements BucketAssigner<T, String> {

    //分区格式
    private String partitionFormat = "dt=%s/hour=%s";

    //获取eventTime实现
    private AssignerEventTimeFunc<T> eventTimeFunc;

    public CommonEventTimeBucketAssigner(AssignerEventTimeFunc<T> eventTimeFunc) {
        this.eventTimeFunc = eventTimeFunc;
    }

    public CommonEventTimeBucketAssigner(String partitionFormat, AssignerEventTimeFunc<T> eventTimeFunc) {
        this.partitionFormat = partitionFormat;
        this.eventTimeFunc = eventTimeFunc;
    }

    @Override
    public String getBucketId(T element, Context context) {
        String partitionValue;
        try {
            partitionValue = getPartitionValue(element);
        } catch (Exception e) {
            partitionValue = "dt=00000000/hour=00";
        }
        return partitionValue;
    }

    private String getPartitionValue(T element) {
        Long eventTime = eventTimeFunc.getEventTime(element);
        Date eventDate = new Date(eventTime);
        return String.format(partitionFormat, new SimpleDateFormat("yyyyMMdd").format(eventDate),
                new SimpleDateFormat("HH").format(eventDate));

    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
