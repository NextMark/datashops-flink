package com.bigdata.datashops.processor.sqlsubmit;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;

import com.bigdata.datashops.processor.config.Config;
import com.bigdata.datashops.processor.sqlsubmit.cli.SqlCommandParser;

public class SqlSubmit {

    public static void main(String[] args) throws Exception {
        Config.initArgs(args);
        String sql = Config.getArgsRequiredValue("sql");

        SqlSubmit submit = new SqlSubmit(sql);
        submit.run();
    }

    // --------------------------------------------------------------------------------------------

    private String sqlFilePath;
    private String workSpace;
    private TableEnvironment tEnv;
    private String sql;

    private SqlSubmit(String sql) {
        this.sql = sql;
    }

    private void run() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        this.tEnv = TableEnvironment.create(settings);
        List<String> sqls = Arrays.asList(sql.trim().substring(0, sql.length()-1).split(";"));
        sqls = sqls.stream().map(x -> x + ";").collect(Collectors.toList());
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sqls);
        for (SqlCommandParser.SqlCommandCall call : calls) {
            callCommand(call);
        }
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            tEnv.executeSql(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }
}
