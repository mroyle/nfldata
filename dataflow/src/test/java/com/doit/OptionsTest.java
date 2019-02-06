package com.doit;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;

public class OptionsTest extends Assert {
    @Test
    public void shoudlBeAbleToGetThePipelineClass(){
        Options options = PipelineOptionsFactory.fromArgs(new String[]{"--runner=DataflowRunner", "--pipeline=DefaultPipeline"})
                .withValidation()
                .as(Options.class);

        assertEquals("DefaultPipeline", options.getPipeline());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfPipelineIsntPassedIn(){
        PipelineOptionsFactory.fromArgs(new String[]{"--runner=DataflowRunner"})
                .withValidation()
                .as(Options.class);
    }
}
