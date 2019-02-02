package com.doit.transformers;

import com.doit.domain.GameResults;
import lombok.Data;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class GameResultsMapperTest extends Assert {

    @Test
    public void shouldCreateGameResultsFromRow(){
        String testLine = "2017090700,NE,KC,1,2017,POST,http://www.nfl.com/liveupdate/game-center/2017090700/2017090700_gtd.json,27,42";

        TestOutputReceiveer<GameResults> r = new TestOutputReceiveer<>();
        GameResultsMapper mapper = new GameResultsMapper();

        mapper.processElement(testLine, r);
        assertNotNull(r.getGeneratedObject());
        assertEquals("2017090700", r.getGeneratedObject().getGame_id());
        assertEquals("NE", r.getGeneratedObject().getHome_team());
        assertEquals("KC", r.getGeneratedObject().getAway_team());
        assertEquals(27, r.getGeneratedObject().getHome_team_score());
        assertEquals(42, r.getGeneratedObject().getAway_team_score());
    }

}
