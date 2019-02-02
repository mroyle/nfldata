package com.doit.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class GameResults implements Serializable {
    private String game_id;
    private String home_team;
    private String away_team;
    private int home_team_score;
    private int away_team_score;
}
