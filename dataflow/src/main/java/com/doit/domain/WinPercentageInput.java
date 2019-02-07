package com.doit.domain;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class WinPercentageInput implements Serializable {
    private PlayResults pr;
    private GameResults gr;

    static public TableSchema getSchema(){
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("team").setType("STRING"));
        fields.add(new TableFieldSchema().setName("drive").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("seconds").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("down").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("yardline").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("yards_to_first").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("score_differential").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("won").setType("INTEGER"));

        return new TableSchema().setFields(fields);
    }

    public TableRow toTableRow() {
        TableRow tr = new TableRow();
        tr.set("team", pr.getTeam());
        tr.set("drive", pr.getDrive_number());
        tr.set("down", pr.getDown());
        tr.set("seconds", pr.getSeconds());
        tr.set("yardline", pr.getYard_line());
        tr.set("yards_to_first", pr.getYards_to_first());
        tr.set("score_differential", pr.getScore_differential());
        tr.set("won", gr.didTeamWin(pr.getTeam()) ? 1 : 0);
        return tr;
    }
}
