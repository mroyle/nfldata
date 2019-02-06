package com.doit.pipelines;

import com.doit.Options;
import com.doit.Pipeline;

public class NoOpPipeline implements Pipeline {
    @Override
    public void process(Options op) {
        NoOpOptions options = (NoOpOptions)op;
        System.out.println("Hi");
    }
}
