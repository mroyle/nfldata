package com.doit.transformers;

import com.doit.domain.PlayResults;
import org.apache.beam.sdk.transforms.DoFn;

public class PlayResultsMapper extends DoFn<String, PlayResults> {

    private int game_id_column=1;
    private int team_column=4;
    private int drive_number_column=15;
    private int down_column=18;
    private int seconds_column=12;
    private int yardline_column=8;
    private int yards_to_first_column=22;
    private int score_differential_column=54;

    @DoFn.ProcessElement
    public void processElement(@DoFn.Element String line, DoFn.OutputReceiver<PlayResults> out) {
        String[] elements = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        try {
            PlayResults ep = new PlayResults(
                    elements[game_id_column],
                    elements[team_column],
                    Integer.parseInt(elements[drive_number_column]),
                    Integer.parseInt(elements[down_column]),
                    Integer.parseInt(elements[seconds_column]),
                    Integer.parseInt(elements[yardline_column]),
                    Integer.parseInt(elements[yards_to_first_column]),
                    Integer.parseInt(elements[score_differential_column]));
            out.output(ep);
        } catch (NumberFormatException e) {

        }
    }
}
