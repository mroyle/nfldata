package com.doit;

import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.v2.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.util.Bytes;

@AllArgsConstructor
public class GameStats {
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
