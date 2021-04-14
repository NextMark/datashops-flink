package com.bigdata.datashops.processor.config;

import org.apache.flink.api.java.utils.ParameterTool;

public class Config {
    private static ParameterTool parameterTool;

    public static void initArgs(String[] args) {
        parameterTool = ParameterTool.fromArgs(args);
    }

    public static String getArgsRequiredValue(String key) {
        return parameterTool.getRequired(key);
    }

    public static String getArgsRequiredValue(String key, String defaultValue) {
        return parameterTool.get(key, defaultValue);
    }
}
