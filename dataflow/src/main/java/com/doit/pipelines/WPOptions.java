package com.doit.pipelines;

import com.doit.Options;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface WPOptions extends Options {
    @Description("list of play by play data")
    @Validation.Required
    String getPlayByPlayInput();
    void setPlayByPlayInput(String value);

    @Description("list of game results data")
    @Validation.Required
    String getGameResultsInput();
    void setGameResultsInput(String value);

    @Description("BigQuery Table")
    @Validation.Required
    String getBigQueryTable();
    void setBigQueryTable(String value);
}
