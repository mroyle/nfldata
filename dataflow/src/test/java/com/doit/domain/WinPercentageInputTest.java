package com.doit.domain;

import com.google.api.services.bigquery.model.TableRow;
import org.junit.Assert;
import org.junit.Test;

public class WinPercentageInputTest extends Assert {
    @Test
    public void shouldTakePlayResultAndGameResultAndDetermineIfTheTeamWon(){
        String game_id = "2009121302";
        String away_teamn = "GB";
        int drive = 4;
        int down = 1;
        int seconds = 3239;
        int yardline = 50;
        int yards_to_first = 10;
        int score_differential = 7;
        String home_team = "CHI";
        int home_team_score = 14;
        int away_team_score = 21;

        PlayResults pr = new PlayResults(game_id, away_teamn, drive, down, seconds, yardline, yards_to_first, score_differential);
        GameResults gr = new GameResults(game_id, home_team, away_teamn, home_team_score, away_team_score);

        WinPercentageInput wp = new WinPercentageInput(pr, gr);
        TableRow tr = wp.toTableRow();
        assertEquals(away_teamn, tr.get("team"));
        assertEquals(drive, tr.get("drive"));
        assertEquals(down, tr.get("down"));
        assertEquals(seconds, tr.get("seconds"));
        assertEquals(yardline, tr.get("yardline"));
        assertEquals(yards_to_first, tr.get("yards_to_first"));
        assertEquals(score_differential, tr.get("score_differential"));
        assertEquals(1, tr.get("won"));
    }
}
