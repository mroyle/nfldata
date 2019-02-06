package com.doit;

import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.lang.reflect.Constructor;

public class Main {

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        Class<?> clazz = Class.forName("com.doit.pipelines." + options.getPipeline());
        Constructor<?> ctor = clazz.getConstructor();
        Pipeline pipeline = (Pipeline) ctor.newInstance();

        pipeline.process(options);
    }

}
