package com.doit.transformers;

import lombok.Data;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

@Data
public class TestOutputReceiveer<T> implements DoFn.OutputReceiver<T> {

    private T generatedObject;

    @Override
    public void output(T output) {
        generatedObject = output;
    }

    @Override
    public void outputWithTimestamp(T output, Instant timestamp) {

    }
}