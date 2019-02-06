package com.doit.pipelines;

import com.doit.Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class PlayerStatsOptionsTest extends Assert {
    private String inputPath = "input";
    private String outputPath = "output";
    private String btInstanceID = "btinstanceid";
    private String btName = "btName";

    private String[] getValidArgs(int[] remove){
        ArrayList<String> argsList = new ArrayList<>(Arrays.asList("--bigTableInstanceID=" + btInstanceID, "--bigTableName="+btName, "--input=" + inputPath, "--output=" + outputPath, "--runner=DataflowRunner", "--pipeline=DefaultPipeline"));

        for (int item : remove){
            argsList.remove(item);
        }
        return argsList.toArray(new String[argsList.size()]);
    }

    @Test
    public void shouldBeAbleToCreateOptionsObjectAndGetInput(){
        PlayerStatsOptions options = PipelineOptionsFactory.fromArgs(getValidArgs(new int[]{}))
                .withValidation()
                .as(PlayerStatsOptions.class);

        assertEquals(inputPath, options.getInput());
    }

    @Test
    public void shouldBeAbleToCreateOptionsObjectAndGetBigQueryTable(){
        PlayerStatsOptions options = PipelineOptionsFactory.fromArgs(getValidArgs(new int[]{}))
                .withValidation()
                .as(PlayerStatsOptions.class);

        assertEquals(outputPath, options.getOutput());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfInputIsMissing(){
        String[] args = getValidArgs(new int[]{2});

        PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PlayerStatsOptions.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfOutputIsMissing(){
        String[] args = getValidArgs(new int[]{3});
        PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PlayerStatsOptions.class);
    }

}
