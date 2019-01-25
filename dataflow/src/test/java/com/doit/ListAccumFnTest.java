package com.doit;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class ListAccumFnTest extends Assert {

    @Test
    public void shouldCreateAccumulatorWith3ElementListOfAllZeros() {
        ListAccumFn accum = new ListAccumFn();
        List<Integer> initialAccum = accum.createAccumulator();
        assertEquals(3, initialAccum.size());
        assertTrue(0==initialAccum.get(0));
        assertTrue(0==initialAccum.get(1));
        assertTrue(0==initialAccum.get(2));
    }

    @Test
    public void shouldBeAbleToAddAnInputToAnAcummulator() {
        ListAccumFn accumFn = new ListAccumFn();
        List<Integer> accum = Stream.of(1, 2, 3).collect(Collectors.toList());
        List<Integer> input = Stream.of(4, 5, 6).collect(Collectors.toList());

        List<Integer> output = accumFn.addInput(accum, input);
        assertEquals(3, accum.size());
        assertTrue(5==output.get(0));
        assertTrue(7==output.get(1));
        assertTrue(9==output.get(2));
    }

    @Test
    public void shouldBeAbleToMergeAccumulators() {
        ListAccumFn accumFn = new ListAccumFn();
        List<Integer> input1 = Stream.of(1, 2, 3).collect(Collectors.toList());
        List<Integer> input2 = Stream.of(4, 5, 6).collect(Collectors.toList());
        List<Integer> input3 = Stream.of(7, 8, 9).collect(Collectors.toList());

        List<List<Integer>> iterable = Stream.of(input1, input2, input3).collect(Collectors.toList());

        List<Integer> results = accumFn.mergeAccumulators(iterable);

        assertEquals(3, results.size());
        assertTrue(12==results.get(0));
        assertTrue(15==results.get(1));
        assertTrue(18==results.get(2));
    }

    @Test
    public void extractOutput() {
        ListAccumFn accumFn = new ListAccumFn();
        List<Integer> input = Stream.of(1, 2, 3).collect(Collectors.toList());

        List<Integer> results = accumFn.extractOutput(input);

        assertEquals(3, results.size());
        assertTrue(1==results.get(0));
        assertTrue(2==results.get(1));
        assertTrue(3==results.get(2));
    }
}