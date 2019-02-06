package com.doit;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface Options extends DataflowPipelineOptions {
    @Description("Which Pipeline to Run")
    @Validation.Required
    String getPipeline();
    void setPipeline(String value);

}