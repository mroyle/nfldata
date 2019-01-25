package com.doit;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;

public class OptionsTest extends Assert {
    private String inputPath = "input";
    private String outputPath = "output";

    @Test
    public void shouldBeAbleToCreateOptionsObjectAndGetInput(){
        String[] args = {"--input=" + inputPath, "--output=" + outputPath, "--runner=DataflowRunner"};
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        assertEquals(inputPath, options.getInput());
    }

    @Test
    public void shouldBeAbleToCreateOptionsObjectAndGetBigQueryTable(){
        String[] args = {"--input=" + inputPath, "--output=" + outputPath, "--runner=DataflowRunner"};
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        assertEquals(outputPath, options.getOutput());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfInputIsMissing(){
        String[] args = {"--output=" + outputPath, "--runner=DataflowRunner"};
        PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfOutputIsMissing(){
        String[] args = {"--input=" + inputPath, "--runner=DataflowRunner"};
        PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
    }

}
