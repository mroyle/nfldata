package com.doit.domain;

import org.junit.Assert;
import org.junit.Test;

public class GameResultsTest extends Assert {
    @Test
    public void shouldCreateCSVLineWithGameID(){
        GameResults gr = new GameResults("id", "home", "away", 1, 2);
        assertEquals("id,home,away,1,2", gr.toCSV(true));
    }

    @Test
    public void shouldCreateCSVLineWithoutGameID(){
        GameResults gr = new GameResults("id", "home", "away", 1, 2);
        assertEquals("home,away,1,2", gr.toCSV(false));
    }

    @Test
    public void shouldBeAbleToTellIfAwayTeamWonGame(){
        GameResults gr = new GameResults("id", "home", "away", 1, 2);
        assertFalse(gr.didTeamWin("home"));
        assertTrue(gr.didTeamWin("away"));
    }

    @Test
    public void shouldBeAbleToTellIfHomeTeamWonGame(){
        GameResults gr = new GameResults("id", "home", "away", 3, 2);
        assertTrue(gr.didTeamWin("home"));
        assertFalse(gr.didTeamWin("away"));
    }

    @Test
    public void shouldBeAbleToTellIfNeitherTeamWonGame(){
        GameResults gr = new GameResults("id", "home", "away", 2, 2);
        assertFalse(gr.didTeamWin("home"));
        assertFalse(gr.didTeamWin("away"));
    }
}
