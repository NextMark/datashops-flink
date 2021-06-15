package com.bigdata.datashops.processor.runner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.bigdata.datashops.processor.config.Config;

public class FlinkSQLRunner {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        Config.initArgs(args);

        String sqls = Config.getArgsRequiredValue("sql");
        String[] sqlArrs = sqls.split(";");
        for (String sql : sqlArrs) {
            tenv.executeSql(sql);
        }

//        tenv.executeSql("CREATE CATALOG iceberg WITH (\n" +
//                                "  'type'='iceberg',\n" +
//                                "  'catalog-type'='hive'," +
//                                "  'uri'='thrift://localhost:9083'," +
//                                "  'warehouse'='hdfs://localhost/user/hive2/warehouse'," +
//                                "  'hive-conf-dir'='/Users/user/work/hive/conf/hive-site.xml'" +
//                                ")");
//
//        tenv.useCatalog("iceberg");
//        tenv.executeSql("CREATE DATABASE iceberg_db");
//        tenv.useDatabase("iceberg_db");
//
//        tenv.executeSql("CREATE TABLE sourceTable (\n" +
//                                " userid int,\n" +
//                                " f_random_str STRING\n" +
//                                ") WITH (\n" +
//                                " 'connector' = 'datagen',\n" +
//                                " 'rows-per-second'='100',\n" +
//                                " 'fields.userid.kind'='random',\n" +
//                                " 'fields.userid.min'='1',\n" +
//                                " 'fields.userid.max'='100',\n" +
//                                "'fields.f_random_str.length'='10'\n" +
//                                ")");
//
//        tenv.executeSql(
//                "insert into iceberg.iceberg_db.iceberg_001 select * from iceberg.iceberg_db.sourceTable");
    }
}
