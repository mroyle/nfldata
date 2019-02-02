package com.doit.transformers;

import com.doit.domain.GameResults;
import org.apache.beam.sdk.transforms.DoFn;

public class GameResultsMapper extends DoFn<String, GameResults> {
    private int game_id_column=0;
    private int home_team_column=1;
    private int away_team_column=2;
    private int home_team_score_column=7;
    private int away_team_score_column=8;

    @DoFn.ProcessElement
    public void processElement(@DoFn.Element String line, DoFn.OutputReceiver<GameResults> out) {
        String[] elements = line.split(",");

        try {
            GameResults gr = new GameResults(
                    elements[game_id_column],
                    elements[home_team_column],
                    elements[away_team_column],
                    Integer.parseInt(elements[home_team_score_column]),
                    Integer.parseInt(elements[away_team_score_column]));
            out.output(gr);
        } catch (NumberFormatException e) {

        }
    }
}
