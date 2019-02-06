package com.doit.pipelines;

import com.doit.Options;
import com.doit.Pipeline;
import com.doit.domain.GameStats;
import com.doit.domain.Play;
import com.doit.transformers.ListAccumFn;
import com.doit.transformers.PlayMapper;
import com.doit.transformers.RunPassFilter;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.transforms.MapElements.into;

public class PlayerStatsPipeline implements Pipeline {

    @Override
    public void process(Options options){
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("team").setType("STRING"));
        fields.add(new TableFieldSchema().setName("opponent").setType("STRING"));
        fields.add(new TableFieldSchema().setName("game_date").setType("DATE"));
        fields.add(new TableFieldSchema().setName("yards").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("touchdowns").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("fumbles").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("player_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("player_name").setType("STRING"));

        TableSchema schema = new TableSchema().setFields(fields);

        BigtableOptions.Builder optionsBuilder =
                new BigtableOptions.Builder()
                        .setProjectId(options.getProject())
                        .setInstanceId(options.getBigTableInstanceID());


        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);

        PCollection<GameStats> plays = pipeline
                .apply("ReadFiles", TextIO.read().from(options.getInput()))
                .apply("Map to Play", ParDo.of(new PlayMapper()))
                .apply("Filter Non-Plays", ParDo.of(new RunPassFilter()))


                .apply("Generate Map", into(
                        TypeDescriptors.kvs(
                                TypeDescriptors.lists(TypeDescriptors.strings()),
                                TypeDescriptors.lists(TypeDescriptors.integers()))
                ).via((Play play) -> play.asListKV()))
                .apply("Collect Stats", Combine.perKey(new ListAccumFn()))
                .apply("Revert to Plays", into(TypeDescriptor.of(GameStats.class))
                        .via((KV<List<String>, List<Integer>> kv) -> new GameStats(
                                kv.getKey().get(0),
                                kv.getKey().get(1),
                                kv.getKey().get(2),
                                kv.getValue().get(0),
                                kv.getValue().get(1),
                                kv.getValue().get(2),
                                kv.getKey().get(3),
                                kv.getKey().get(4)
                        )));



        plays.apply("Format PlayResults for BigQuery", into(TypeDescriptor.of(TableRow.class))
                .via((GameStats yards) -> yards.toTableRow()))
                .apply(BigQueryIO.writeTableRows()
                        .to(options.getOutput())
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));


        CloudBigtableTableConfiguration c =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId(options.getProject())
                        .withInstanceId(options.getBigTableInstanceID())
                        .withTableId(options.getBigTableName())
                        .build();

        plays.apply("Format PlayResults for BigTable", into(TypeDescriptors.kvs(TypeDescriptor.of(ByteString.class), TypeDescriptors.iterables(TypeDescriptor.of(Mutation.class))))
                .via((GameStats yards) -> yards.asBigTableRow()))
                .apply("write to  BigTable",
                        BigtableIO.write()
                                .withBigtableOptions(optionsBuilder)
                                .withTableId("player_yards"));

        PipelineResult result = pipeline.run();
        result.waitUntilFinish();
        if (!PipelineResult.State.DONE.equals(result.getState())){
            throw new RuntimeException("Pipeline didn't complete successfully.");
        }

    }
}
