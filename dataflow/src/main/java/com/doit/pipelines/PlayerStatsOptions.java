package com.doit.pipelines;

import com.doit.Options;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface PlayerStatsOptions extends Options {
    @Description("Comma Delimited List of input files")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("BigQuery table name")
    @Validation.Required
    String getOutput();
    void setOutput(String value);

    @Description("BigTable instance id")
    @Validation.Required
    String getBigTableInstanceID();
    void setBigTableInstanceID(String value);

    @Description("BigTable table name")
    String getBigTableName();
    void setBigTableName(String value);

}
