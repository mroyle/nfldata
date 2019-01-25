package com.doit;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface Options extends DataflowPipelineOptions {
    @Description("Comma Delimited List of input files")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("BigQuery table name")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
}