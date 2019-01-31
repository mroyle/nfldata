package com.doit.transformers;

import com.doit.domain.Play;
import org.apache.beam.sdk.transforms.DoFn;

public class RunPassFilter extends DoFn<Play, Play> {
    @DoFn.ProcessElement
    public void processElement(@DoFn.Element Play ty, DoFn.OutputReceiver<Play> out) {
        if (ty.isRunOrPass()) {
            out.output(ty);
        }
    }
}