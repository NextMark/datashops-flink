package com.bigdata.datashops.processor.sqlsubmit.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CliOptionsParser {
    private static final Option OPTION_SQL_FILE = Option.builder("f").required(true) //参数是否必须
                                                          .longOpt("file").numberOfArgs(1).argName("SQL file path")
                                                          .desc("The SQL file path.").build();

    private static final Options CLIENT_OPTIONS = getClientOptions(new Options());

    private static Options getClientOptions(Options options) {
        options.addOption(OPTION_SQL_FILE);
        //        options.addOption(OPTION_WORKING_SPACE);
        return options;
    }

    // --------------------------------------------------------------------------------------------
    //  Line Parsing
    // --------------------------------------------------------------------------------------------

    public static CliOptions parseClient(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException("./sql-submit -f <sql-file>");
        }
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(CLIENT_OPTIONS, args, true);
            return new CliOptions(line.getOptionValue(CliOptionsParser.OPTION_SQL_FILE.getOpt()));
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
