package com.bigdata.datashops.processor.runner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.datashops.processor.config.Config;

public class FlinkSQLRunner {
    private static Logger LOG = LoggerFactory.getLogger(FlinkSQLRunner.class);

    public static void main(String[] args) throws Exception {
        Config.initArgs(args);
        String sqls = Config.getArgsRequiredValue("sql");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        String[] sqlArrs = sqls.split(";");
        for (String sql : sqlArrs) {
            LOG.info("sub sql:\n{}", sql.trim());
            tableEnvironment.executeSql(sql.trim());
        }

//        String test1 = "CREATE TABLE datagen (\n" +
//                                   " userid int,\n" +
//                                   " proctime as PROCTIME()\n" +
//                                   ") WITH (\n" +
//                                   " 'connector' = 'datagen',\n" +
//                                   " 'rows-per-second'='100',\n" +
//                                   " 'fields.userid.kind'='random',\n" +
//                                   " 'fields.userid.min'='1',\n" +
//                                   " 'fields.userid.max'='100'\n" +
//                                   ")";
//
//        tableEnvironment.executeSql(test1);
//
//        String test2 = "CREATE TABLE test2 (\n" + "  day_str string,\n" + "  pv bigint,\n" + "  PRIMARY KEY (day_str) NOT ENFORCED\n" + ") WITH (\n"
//                               + "   'connector' = 'jdbc',\n" + "   'url' = 'jdbc:mysql://192.168.6.34:3306/bigdata',\n"
//                               + "   'table-name' = 'test2',\n" + "   'username' = 'bigdata_rw',\n"
//                               + "   'password' = 'ZZPp8PNyyuMCocy2rocA'\n" + ")";
//
//        tableEnvironment.executeSql(test2);
//        String in = "insert into test2 SELECT DATE_FORMAT(proctime, 'yyyy-MM-dd') as day_str, count(*) FROM datagen "
//                            + "GROUP BY DATE_FORMAT(proctime, 'yyyy-MM-dd')";
//        tableEnvironment.executeSql(in);

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
