package com.doit;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

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

        Pipeline pipeline = Pipeline.create(options);
        pipeline
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
                                )))

                .apply("Format PlayResults", into(TypeDescriptor.of(TableRow.class))
                        .via((Play yards) -> yards.toTableRow()))
                .apply(BigQueryIO.writeTableRows()
                        .to(options.getOutput())
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        pipeline.run().waitUntilFinish();
    }
}
