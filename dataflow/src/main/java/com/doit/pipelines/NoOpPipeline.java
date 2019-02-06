package com.doit.pipelines;

import com.doit.Options;
import com.doit.Pipeline;

public class NoOpPipeline implements Pipeline {
    @Override
    public void process(Options options) {
        System.out.println("Hi");
    }
}
