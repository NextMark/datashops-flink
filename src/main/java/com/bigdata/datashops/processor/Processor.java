package com.bigdata.datashops.processor;

import java.io.File;
import java.util.Objects;
import java.util.Properties;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.TernaryBoolean;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.datashops.processor.config.Config;
import com.bigdata.datashops.processor.assigner.CommonEventTimeBucketAssigner;
import com.bigdata.datashops.processor.format.ParquetAvroWriters;
import com.bigdata.datashops.processor.pojo.Record;

public class Processor {
    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    public static void main(String[] args) throws Exception {
        Config.initArgs(args);

        String kafkaServer = Config.getArgsRequiredValue("kafkaServer");
        String jobName = Config.getArgsRequiredValue("jobName");

        String topic = Config.getArgsRequiredValue("topic");
        String groupId = Config.getArgsRequiredValue("groupId");
        String parallelism = Config.getArgsRequiredValue("parallelism", "1");
        String checkpointPath = Config.getArgsRequiredValue("checkpointPath");
        String checkpointInterval = Config.getArgsRequiredValue("checkpointInterval");
        String location = Config.getArgsRequiredValue("path");
        String timeField = Config.getArgsRequiredValue("ts");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(parallelism));

        int checkpointTime = Integer.parseInt(checkpointInterval);
        env.enableCheckpointing(checkpointTime * 60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTime * 2 * 60 * 1000);

        FsStateBackend fsStateBackend =
                new FsStateBackend(checkpointPath + "/" + jobName + "/" + topic);
        StateBackend rocksDBBackend = new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE);
        env.setStateBackend(rocksDBBackend);

        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.minutes(3)));

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaServer);
        props.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        DataStream<String> stream = env.addSource(consumer);

        DataStream<Record> source = stream.map(event -> {
            Record record = new Record();
            record.content = event;
            return record;
        }).filter(e -> !Objects.isNull(JSONObject.parseObject(e.content).get(timeField)));

        OutputFileConfig config =
                OutputFileConfig.builder().withPartSuffix("." + topic.trim() + ".snappy.parquet").build();

        StreamingFileSink<Record> sink = StreamingFileSink
                                                 .forBulkFormat(new org.apache.flink.core.fs.Path("hdfs://" + location),
                                                         ParquetAvroWriters.forReflectRecord(Record.class,
                                                                 CompressionCodecName.SNAPPY)).withBucketAssigner(
                        new CommonEventTimeBucketAssigner<>("dt=%s/hour=%s",
                                e -> JSONObject.parseObject(e.content).getLong(timeField))).withOutputFileConfig(config)
                                                 .build();
        source.addSink(sink);
        env.execute(jobName);
    }
}
