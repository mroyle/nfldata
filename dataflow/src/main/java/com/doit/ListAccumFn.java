package com.doit;

import org.apache.beam.sdk.transforms.Combine;

import java.util.Arrays;
import java.util.List;

public class ListAccumFn extends Combine.CombineFn<List<Integer>, List<Integer>, List<Integer>> {
    @Override
    public List<Integer> createAccumulator() {
        Integer[] ints = {0,0,0};
        return Arrays.asList(ints);
    }

    @Override
    public List<Integer> addInput(List<Integer> accumulator, List<Integer> input) {
        Integer[] ints = {accumulator.get(0) + input.get(0),accumulator.get(1) + input.get(1),accumulator.get(2) + input.get(2)};
        return Arrays.asList(ints);
    }

    @Override
    public List<Integer> mergeAccumulators(Iterable<List<Integer>> accumulators) {
        List<Integer> accum = createAccumulator();
        for (List<Integer> result: accumulators){
            accum = addInput(accum, result);
        }
        return accum;
    }

    @Override
    public List<Integer> extractOutput(List<Integer> accumulator) {
        return accumulator;
    }
}
