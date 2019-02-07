package com.doit.domain;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.bigtable.v2.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class GameStats implements Serializable {
    private String team;
    private String opponent;
    private String game_date;
    private int yards_gained;
    private int touchdowns;
    private int fumbles;
    private String player_id;
    private String player_name;

    public KV<ByteString, Iterable<Mutation>> asBigTableRow(){
        String key = player_id + "#" + game_date;

        Iterable<Mutation> mutations =
                ImmutableList.of(Mutation.newBuilder()
                        .setSetCell(
                                Mutation.SetCell.newBuilder()
                                        .setFamilyName("details")
                                        .setColumnQualifier(ByteString.copyFromUtf8("opponent"))
                                        .setValue(ByteString.copyFrom(Bytes.toBytes(opponent)))
                        ).setSetCell(
                                Mutation.SetCell.newBuilder()
                                        .setFamilyName("details")
                                        .setColumnQualifier(ByteString.copyFromUtf8("yards"))
                                        .setValue(ByteString.copyFrom(Bytes.toBytes(yards_gained)))
                        )
                        .build());
        return KV.of(ByteString.copyFromUtf8(key), mutations);
    }

    static public TableSchema getSchema(){
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("team").setType("STRING"));
        fields.add(new TableFieldSchema().setName("opponent").setType("STRING"));
        fields.add(new TableFieldSchema().setName("game_date").setType("DATE"));
        fields.add(new TableFieldSchema().setName("yards").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("touchdowns").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("fumbles").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("player_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("player_name").setType("STRING"));

        return new TableSchema().setFields(fields);
    }

    public TableRow toTableRow(){

        TableRow tr = new TableRow();
        tr.set("team", team);
        tr.set("opponent", opponent);
        tr.set("game_date", game_date);
        tr.set("yards", yards_gained);
        tr.set("touchdowns", touchdowns);
        tr.set("fumbles", fumbles);
        tr.set("player_id", player_id);
        tr.set("player_name", player_name);
        return tr;
    }
}
