package com.doit.domain;

import com.doit.domain.GameStats;
import com.google.api.services.bigquery.model.TableRow;
import org.junit.Assert;
import org.junit.Test;

public class GameStatsTest extends Assert {

    @Test
    public void shouldBeAbleToCreateATableRow(){
        GameStats passPlay = new GameStats("oak", "den", "2018-12-08", 10, 1, 0, "1", "mikePass");

        TableRow passRow = passPlay.toTableRow();
        assertEquals("oak", passRow.get("team"));
        assertEquals("den", passRow.get("opponent"));
        assertEquals("2018-12-08", passRow.get("game_date"));
        assertEquals("1", passRow.get("player_id"));
        assertEquals("mikePass", passRow.get("player_name"));
        assertEquals(new Integer(10), passRow.get("yards"));
        assertEquals(new Integer(1), passRow.get("touchdowns"));
        assertEquals(new Integer(0), passRow.get("fumbles"));
    }
}
