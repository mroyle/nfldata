package com.doit;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

import javax.swing.text.TableView;
import java.util.List;

public class PlayTest extends Assert {
    @Test
    public void shouldBeAbleToConvertToCSV(){
        Play ty = new Play("oak", "den", "2018-12-08", 10, 1, 0, "1", "mike", "NA", "NA");
        assertEquals("oak,den,2018-12-08,10,1,0,1,mike", ty.toCSV());
    }

    @Test
    public void shouldntBeValidIfAllRushingAndReceivingPlayerInfoIsNA(){
        Play validTY = new Play("oak", "den", "2018-12-08", 10, 1, 0, "1", "mike", "NA", "NA");
        Play invalidTY = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "NA", "NA", "NA");

        assertTrue(validTY.isRunOrPass());
        assertFalse(invalidTY.isRunOrPass());
    }

    @Test
    public void shouldConstructATableRow(){
        Play validTY = new Play("oak", "den", "2018-12-08", 10, 1, 0, "1", "mike", "NA", "NA");

    }

    @Test
    public void shouldBeAbleToDetermineIfThePlayerIsARReceiverOrRusher(){
        Play passPlayWithOutID = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "mike", "NA", "NA");
        Play passPlayWithOutName = new Play("oak", "den", "2018-12-08", 10, 1, 0, "1", "NA", "NA", "NA");
        Play rushPlayWithOutID = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "NA", "NA", "Mike");
        Play rushPlayWithOutName = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "NA", "1", "NA");

        assertFalse(passPlayWithOutID.hasReceiver());
        assertFalse(passPlayWithOutName.hasReceiver());
        assertFalse(rushPlayWithOutID.hasRusher());
        assertFalse(rushPlayWithOutName.hasRusher());
    }

    @Test
    public void shouldBeAbleToDetermineIfThePlayIsARunOrPass(){
        Play passPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "1", "mikePass", "NA", "NA");
        Play rushPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "NA", "1", "mikeRush");
        Play neitherPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "NA", "NA", "NA");

        assertTrue(passPlay.isRunOrPass());
        assertTrue(rushPlay.isRunOrPass());
        assertFalse(neitherPlay.isRunOrPass());
    }

    @Test
    public void shouldBeAbleToPlayerNameAndID(){
        Play passPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "1", "mikePass", "NA", "NA");
        Play rushPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "NA", "NA", "2", "mikeRush");

        assertEquals("mikePass", passPlay.getPlayerName());
        assertEquals("mikeRush", rushPlay.getPlayerName());
        assertEquals("1", passPlay.getPlayerID());
        assertEquals("2", rushPlay.getPlayerID());
    }

    @Test
    public void shouldBeAbleToTurnPlayIntoKayValuePair(){
        Play passPlay = new Play("oak", "den", "2018-12-08", 10, 1, 0, "1", "mikePass", "NA", "NA");

        KV<List<String>, List<Integer>> kv = passPlay.asListKV();

        List<String> key = kv.getKey();
        List<Integer> value = kv.getValue();

        assertEquals(5, key.size());
        assertEquals("oak", key.get(0));
        assertEquals("den", key.get(1));
        assertEquals("2018-12-08", key.get(2));
        assertEquals("1", key.get(3));
        assertEquals("mikePass", key.get(4));

        assertEquals(3, value.size());
        assertEquals(new Integer(10), value.get(0));
        assertEquals(new Integer(1), value.get(1));
        assertEquals(new Integer(0), value.get(2));
    }

}
