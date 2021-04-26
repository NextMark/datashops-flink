package com.bigdata.datashops.processor.sqlsubmit.cli;

public class CliOptions {
    private final String sqlFilePath;

    public CliOptions(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }
}
