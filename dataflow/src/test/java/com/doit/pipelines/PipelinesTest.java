package com.doit.pipelines;

import com.doit.Options;
import com.doit.Pipeline;
import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class PipelinesTest extends Assert {
    @Test
    public void allPipelinesShouldHaveAnOptionsClass(){
        Reflections reflections = new Reflections("com.doit.pipelines", new SubTypesScanner(false));
        Set<Class<? extends Pipeline>> allPipelines = reflections.getSubTypesOf(Pipeline.class);
        Set<String> allOptions = reflections.getSubTypesOf(Options.class).stream().map(s -> s.getCanonicalName()).collect(Collectors.toSet());
        for (Class clazz : allPipelines){
            assertTrue(clazz.getCanonicalName() + " doesn't have a corresponding options interface", allOptions.contains(clazz.getCanonicalName().replaceAll("Pipeline", "Options")));
        }

    }
}
