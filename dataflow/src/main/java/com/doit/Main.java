package com.doit;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import com.doit.Options;

import java.lang.reflect.Constructor;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) throws Exception {
        ArrayList<String> pipelineOnly = new ArrayList<>();
        for (String a : args){
            if (a.startsWith("--pipeline")){
                pipelineOnly.add(a);
            }
        }

        Options options = PipelineOptionsFactory.fromArgs(pipelineOnly.toArray(new String[1]))
                .as(Options.class);

        Class<?> clazz = Class.forName("com.doit.pipelines." + options.getPipeline() + "Pipeline");
        Constructor<?> ctor = clazz.getConstructor();
        Pipeline pipeline = (Pipeline) ctor.newInstance();

        pipeline.process(PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as((Class<? extends Options>)Class.forName("com.doit.pipelines." + options.getPipeline() + "Options")));
    }

}
