package com.doit;

import org.apache.beam.sdk.transforms.DoFn;

public class PlayMapper extends DoFn<String, Play> {
    @DoFn.ProcessElement
    public void processElement(@DoFn.Element String line, DoFn.OutputReceiver<Play> out) {
        // Use OutputReceiver.output to emit the output element.
        String[] elements = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        try {
            Play ty = new Play(elements[4], elements[6], elements[9], Integer.parseInt(elements[26]), Integer.parseInt(elements[144]), Integer.parseInt(elements[153]), elements[162], elements[163], elements[164], elements[165]);
            out.output(ty);
        }catch (NumberFormatException nfe){

        }
    }
}