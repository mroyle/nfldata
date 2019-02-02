package com.doit.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ExpectedPointsInput implements Serializable {
    private String game_id;
    private String team;
    private int drive_number;
    private int down;
    private int seconds;
    private int yard_line;
    private int yards_to_first;
    private int score_differential;
}
