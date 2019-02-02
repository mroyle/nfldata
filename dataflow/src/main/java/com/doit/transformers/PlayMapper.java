package com.doit.transformers;

import com.doit.domain.Play;
import org.apache.beam.sdk.transforms.DoFn;

public class PlayMapper extends DoFn<String, Play> {
    private int posession_team_column = 4;
    private int defensive_team_column = 6;
    private int game_date_column = 9;
    private int yards_gained_column = 26;
    private int touchdown_column = 144;
    private int fumble_column = 153;
    private int receiver_player_id_column = 162;
    private int receiver_player_name_column = 163;
    private int rusher_player_id_column = 164;
    private int rusher_player_name_column = 165;

    @DoFn.ProcessElement
    public void processElement(@DoFn.Element String line, DoFn.OutputReceiver<Play> out) {
        // Use OutputReceiver.output to emit the output element.
        String[] elements = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        //offset is needed because they added touchback as the 119 element in 2016 so any columns >=119 needs to apply offset
        int offset = Math.abs(255 - elements.length);
        try {
            Play ty = new Play(
                    elements[posession_team_column],
                    elements[defensive_team_column],
                    elements[game_date_column],
                    Integer.parseInt(elements[yards_gained_column]),
                    Integer.parseInt(elements[touchdown_column + offset]),
                    Integer.parseInt(elements[fumble_column + offset]),
                    elements[receiver_player_id_column + offset],
                    elements[receiver_player_name_column + offset],
                    elements[rusher_player_id_column + offset],
                    elements[rusher_player_name_column + offset]);
            out.output(ty);
        }catch (NumberFormatException nfe){

        }
    }
}