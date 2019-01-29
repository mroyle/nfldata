package com.doit;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class OptionsTest extends Assert {
    private String inputPath = "input";
    private String outputPath = "output";
    private String btInstanceID = "btinstanceid";
    private String btName = "btName";

    private String[] getValidArgs(int[] remove){
        ArrayList<String> argsList = new ArrayList<>(Arrays.asList("--bigTableInstanceID=" + btInstanceID, "--bigTableName="+btName, "--input=" + inputPath, "--output=" + outputPath, "--runner=DataflowRunner"));

        for (int item : remove){
            argsList.remove(item);
        }
        return argsList.toArray(new String[argsList.size()]);
    }

    @Test
    public void shouldBeAbleToCreateOptionsObjectAndGetInput(){
        Options options = PipelineOptionsFactory.fromArgs(getValidArgs(new int[]{}))
                .withValidation()
                .as(Options.class);

        assertEquals(inputPath, options.getInput());
    }

    @Test
    public void shouldBeAbleToCreateOptionsObjectAndGetBigQueryTable(){
        Options options = PipelineOptionsFactory.fromArgs(getValidArgs(new int[]{}))
                .withValidation()
                .as(Options.class);

        assertEquals(outputPath, options.getOutput());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfInputIsMissing(){
        String[] args = getValidArgs(new int[]{2});

        PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfOutputIsMissing(){
        String[] args = getValidArgs(new int[]{3});
        PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
    }

}
