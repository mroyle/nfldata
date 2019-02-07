package com.doit.pipelines;

import com.doit.Options;
import com.doit.Pipeline;
import com.doit.domain.PlayResults;
import com.doit.domain.GameResults;
import com.doit.domain.WinPercentageInput;
import com.doit.transformers.PlayResultsMapper;
import com.doit.transformers.GameResultsMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.beam.sdk.transforms.MapElements.into;

public class WPPipeline implements Pipeline {
    @Override
    public void process(Options op) {
        WPOptions options = (WPOptions)op;

        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);

        PCollection<PlayResults> epInput = pipeline
                .apply("Read Play by Play Files", TextIO.read().from(options.getPlayByPlayInput()))
                .apply("Map to PlayResults", ParDo.of(new PlayResultsMapper()));

        PCollection<GameResults> gameResults = pipeline
                .apply("Read Game Results Files", TextIO.read().from(options.getGameResultsInput()))
                .apply("Map to GameResults", ParDo.of(new GameResultsMapper()));

        PCollection<org.apache.beam.sdk.values.KV<String, PlayResults>> epKV = epInput.apply(into(
                TypeDescriptors.kvs(
                        TypeDescriptors.strings(),
                        TypeDescriptor.of(PlayResults.class))
        ).via((PlayResults input) -> org.apache.beam.sdk.values.KV.of(input.getGame_id(), input)));

        PCollection<org.apache.beam.sdk.values.KV<String, GameResults>> gameKV = gameResults.apply(into(
                TypeDescriptors.kvs(
                        TypeDescriptors.strings(),
                        TypeDescriptor.of(GameResults.class))
        ).via((GameResults input) -> org.apache.beam.sdk.values.KV.of(input.getGame_id(), input)));

        final TupleTag<PlayResults> t1 = new TupleTag<>();
        final TupleTag<GameResults> t2 = new TupleTag<>();
        PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(t1, epKV)
                        .and(t2, gameKV)
                        .apply(CoGroupByKey.<String>create());

        coGbkResultCollection.apply(into(
               TypeDescriptors.lists(
                    TypeDescriptors.kvs(
                        TypeDescriptor.of(PlayResults.class),
                        TypeDescriptor.of(GameResults.class))))
                .via((KV<String, CoGbkResult> kv) -> {
                    GameResults gr = kv.getValue().getOnly(t2);
                    return StreamSupport.stream(kv.getValue().getAll(t1).spliterator(), false)
                            .map((PlayResults ep) -> org.apache.beam.sdk.values.KV.of(ep, gr))
                            .collect(Collectors.toList());

                }
            ))
                .apply(Flatten.iterables())
                .apply(into(TypeDescriptor.of(WinPercentageInput.class)).via((KV<PlayResults, GameResults> kv) -> new WinPercentageInput(kv.getKey(), kv.getValue())))
                .apply(into(TypeDescriptor.of(TableRow.class)).via((WinPercentageInput wp) -> wp.toTableRow()))
                .apply(BigQueryIO.writeTableRows()
                        .to(options.getBigQueryTable())
                        .withSchema(WinPercentageInput.getSchema())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        pipeline.run();
    }
}
