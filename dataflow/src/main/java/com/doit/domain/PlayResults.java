package com.doit.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class PlayResults implements Serializable {
    private String game_id;
    private String team;
    private int drive_number;
    private int down;
    private int seconds;
    private int yard_line;
    private int yards_to_first;
    private int score_differential;

    public String toCSV(boolean includeGameID){
        String value = includeGameID ? game_id + "," : "";
        return value + String.join(",", new String[]{
                team,
                String.valueOf(drive_number),
                String.valueOf(down),
                String.valueOf(seconds),
                String.valueOf(yard_line),
                String.valueOf(yards_to_first),
                String.valueOf(score_differential)});
    }
}
