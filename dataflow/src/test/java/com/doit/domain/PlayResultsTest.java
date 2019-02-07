package com.doit.domain;

import org.junit.Assert;
import org.junit.Test;

public class PlayResultsTest extends Assert {
    @Test
    public void shouldConstructCSVLineWithGameID(){
        PlayResults ep = new PlayResults("id", "team", 1, 2, 3, 4, 5, 6);
        assertEquals("id,team,1,2,3,4,5,6", ep.toCSV(true));
    }

    @Test
    public void shouldConstructCSVLineWithoutGameID(){
        PlayResults ep = new PlayResults("id", "team", 1, 2, 3, 4, 5, 6);
        assertEquals("team,1,2,3,4,5,6", ep.toCSV(false));
    }
}
