package com.doit;

import com.google.api.services.bigquery.model.TableRow;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
@Data
public class Play implements Serializable {

    private static String EMPTY_PLAYER="NA";

    private String posession_team;
    private String defensive_team;
    private String game_date;
    private int yards_gained;
    private int touchdown;
    private int fumble;
    private String receiver_player_id;
    private String receiver_player_name;
    private String rusher_player_id;
    private String rusher_player_name;


    public TableRow toTableRow(){

        TableRow tr = new TableRow();
        tr.set("team", posession_team);
        tr.set("opponent", defensive_team);
        tr.set("game_date", game_date);
        tr.set("yards", yards_gained);
        tr.set("touchdowns", touchdown);
        tr.set("fumbles", fumble);
        tr.set("player_id", getPlayerID());
        tr.set("player_name", getPlayerName());
        return tr;
    }

    public String toCSV() {
        return  posession_team +
                "," + defensive_team +
                "," + game_date +
                "," + yards_gained +
                "," + touchdown +
                "," + fumble +
                "," + getPlayerID() +
                "," + getPlayerName();
    }

    public KV<List<String>, List<Integer>> asListKV(){
        String[] key = {posession_team, defensive_team, game_date, getPlayerID(), getPlayerName()};
        Integer[] value = {yards_gained, touchdown, fumble};
        return KV.of(Arrays.asList(key), Arrays.asList(value));
    }

    public boolean isRunOrPass() {
        return  hasReceiver() || hasRusher();
    }

    protected boolean hasReceiver(){
        return !(receiver_player_id.equalsIgnoreCase(EMPTY_PLAYER) ||
                receiver_player_name.equalsIgnoreCase(EMPTY_PLAYER));
    }

    protected boolean hasRusher(){
        return !(rusher_player_id.equalsIgnoreCase(EMPTY_PLAYER) ||
                rusher_player_name.equalsIgnoreCase(EMPTY_PLAYER));

    }

    String getPlayerID(){
        return hasReceiver() ? receiver_player_id : rusher_player_id;
    }

    String getPlayerName(){
        return hasReceiver() ? receiver_player_name : rusher_player_name;
    }

}
