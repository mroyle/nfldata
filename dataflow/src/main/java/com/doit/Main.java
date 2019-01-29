package com.doit;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.transforms.MapElements.*;

public class Main {

    public static void main(String[] args){
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        options.setRunner(options.getRunner());

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


        Pipeline pipeline = Pipeline.create(options);

        PCollection<Play> plays = pipeline
                .apply("ReadFiles", TextIO.read().from(options.getInput()))
                .apply("Map to Play", ParDo.of(new PlayMapper()))
                .apply("Filter Non-Plays", ParDo.of(new RunPassFilter()))


                .apply("Generate Map", into(
                        TypeDescriptors.kvs(
                                TypeDescriptors.lists(TypeDescriptors.strings()),
                                TypeDescriptors.lists(TypeDescriptors.integers()))
                        ).via((Play play) -> play.asListKV()))
                .apply("Collect Stats", Combine.perKey(new ListAccumFn()))
                .apply("Revert to Plays", into(TypeDescriptor.of(Play.class))
                        .via((KV<List<String>, List<Integer>> kv) -> new Play(
                                                                    kv.getKey().get(0),
                                                                    kv.getKey().get(1),
                                                                    kv.getKey().get(2),
                                                                    kv.getValue().get(0),
                                                                    kv.getValue().get(1),
                                                                    kv.getValue().get(2),
                                                                    kv.getKey().get(3),
                                                                    kv.getKey().get(4), "NA", "NA"
                                )));



                plays.apply("Format PlayResults for BigQuery", into(TypeDescriptor.of(TableRow.class))
                        .via((Play yards) -> yards.toTableRow()))
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
                            .via((Play yards) -> yards.asBigTableRow()))
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
