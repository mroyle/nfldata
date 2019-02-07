package com.doit.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@AllArgsConstructor
@ToString
public class GameResults implements Serializable {
    private String game_id;
    private String home_team;
    private String away_team;
    private int home_team_score;
    private int away_team_score;

    public String toCSV(boolean includeGameID){
        String value = includeGameID ? game_id + "," : "";
        return value + String.join(",", new String[]{home_team, away_team, String.valueOf(home_team_score), String.valueOf(away_team_score)});
    }

    public boolean didTeamWin(String team) {
        if (home_team_score > away_team_score){
            return team.equalsIgnoreCase(home_team);
        }else if (away_team_score > home_team_score){
            return team.equalsIgnoreCase(away_team);
        }else{
            return false;
        }
    }
}
