package com.doit.transformers;

import com.doit.domain.Play;
import lombok.Data;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class RunPassFilterTest extends Assert {
    @Test
    public void shouldNotFilterOutPassPlays(){
        Play passPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "1", "mikePass", "NA", "NA");

        RunPassFilter filter = new RunPassFilter();

        PlayOutputReceiver output = new PlayOutputReceiver();
        filter.processElement(passPlay, output);
        assertNotNull(output.outputPlayObject);
        assertEquals(passPlay, output.outputPlayObject);
    }

    @Test
    public void shouldNotFilterOutRushPlays(){
        Play rushPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "NA", "1", "mikeRush");

        RunPassFilter filter = new RunPassFilter();

        PlayOutputReceiver output = new PlayOutputReceiver();
        filter.processElement(rushPlay, output);
        assertNotNull(output.outputPlayObject);
        assertEquals(rushPlay, output.outputPlayObject);
    }

    @Test
    public void shouldFilterOutNonPassOrRushPlays(){
        Play neitherPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "NA", "NA", "NA");

        RunPassFilter filter = new RunPassFilter();

        PlayOutputReceiver output = new PlayOutputReceiver();
        filter.processElement(neitherPlay, output);
        assertNull(output.outputPlayObject);
    }

    @Data
    private static class PlayOutputReceiver implements DoFn.OutputReceiver<Play> {

        private Play outputPlayObject = null;

        @Override
        public void output(Play output) {
            outputPlayObject = output;
        }

        @Override
        public void outputWithTimestamp(Play output, Instant timestamp) {

        }
    }

}
