/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.ts.TimeSerie;
import org.m2mp.db.ts.TimedData;
import org.m2mp.db.ts.TimedDataWrapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Florent Clairambault
 */
public class TimeSerieTest {
//	private static DB db;

    @BeforeClass
    public static void setUpClass() {
        DB.keyspace("ks_test", true);
        try {
            DB.execute("drop table TimeSeries;");
        } catch (Exception ex) {
        }
        TimeSerie.prepareTable();
    }

    @Test
    public void insertStringNoType() {
        String id = "insert-without-type";
        TimeSerie.save(new TimedData(id, null, "{\"lat\":48.8,\"lon\":2.5,\"_type\":\"loc\"}"));
    }

    @Test
    public void insert1000Strings() throws InterruptedException {
        String id = "insert-with-type";
        Date d = new Date();
        for (int i = 0; i < 1000; i++) {
            d.setTime(d.getTime() + 100);
            TimeSerie.save(new TimedData(id, "loc", "{\"lat\":48.8,\"lon\":2.5}"));
        }
    }

    @Test
    public void insertMap() {
        String id = "insert-map";
        Date d1 = new Date(1337000000000L);
        Date d2 = new Date(1338000000000L);
        Map<String, Object> map = new TreeMap<>();
        map.put("lat", 48.8);
        map.put("lon", 2.5);
        TimeSerie.save(new TimedData(id, "loc", d1, map));

        Map<String, Object> map2 = new TreeMap<>();
        map2.put("previous", map);
        map2.put("lat", 45.8);
        map2.put("lon", 1.5);
        TimeSerie.save(new TimedData(id, "loc", d2, map2));
    }

    private long PERIOD = 47 * 3600 * 1000;

    private void insertData(String id) {
        int COUNT = 100;
        long begin = System.currentTimeMillis();
        for (long i = 1; i <= COUNT; i++) {
            long t = begin - (i * PERIOD);
            Date d = new Date(t);
            Map<String, Object> map = new TreeMap<>();
            map.put("time", t);
            map.put("date", "" + d);
            TimeSerie.save(new TimedData(id, null, d, map));
        }
    }

    @Test
    public void searchAll() {
        String id = "dev-" + UUID.randomUUID();
//		System.out.println("ID: " + id);
        insertData(id);
        {
            int nb = 0;
            Date lastDate = null;
            for (TimedData td : TimeSerie.getData(id, null, (Date) null, (Date) null, true)) {
                nb++;
//				System.out.println("[DESC] --> " + td.getJson());
                if (lastDate != null) {
                    Assert.assertTrue(lastDate.after(td.getDate()));
                }
                lastDate = td.getDate();
            }
            Assert.assertEquals(100, nb);
        }
        {
            int nb = 0;
            Date lastDate = null;
            for (TimedData td : TimeSerie.getData(id, null, (Date) null, (Date) null, false)) {
                nb++;
//				System.out.println("[ASC] --> " + td.getJson());
                if (lastDate != null) {
                    Assert.assertTrue(lastDate.before(td.getDate()));
                }
                lastDate = td.getDate();
            }
            Assert.assertEquals(100, nb);
        }
    }

    @Test
    public void searchOneByOneDesc() {
        String id = "dev-" + UUID.randomUUID();
//		System.out.println("ID: " + id);
        insertData(id);
        searchOneByOne(id, true);
    }

    @Test
    public void searchOneByOneAsc() {
        String id = "dev-" + UUID.randomUUID();
//		System.out.println("ID: " + id);
        insertData(id);
        searchOneByOne(id, false);
    }

    public void searchOneByOne(String id, boolean inverted) {
        TimedData td = null;
        boolean ok = true;
        int nb = 0;
        for (TimedData td2 : TimeSerie.getData(id, null, (Date) null, (Date) null, inverted)) {
            td = td2;
            nb++;
            break;
        }
//		System.out.println("First: " + td.getJson());
        Date lastDate = null;
        while (ok) {
            ok = false;
            for (TimedData td2 : TimeSerie.getData(id, null, inverted ? null : td.getDateUUID(), inverted ? td.getDateUUID() : null, inverted)) {
//				System.out.println("Previous: " + td.getJson());
                td = td2;
                ok = true;
                nb++;
                if (lastDate != null) {
                    if (inverted) {
                        Assert.assertTrue(lastDate.after(td.getDate()));
                    } else {
                        Assert.assertTrue(lastDate.before(td.getDate()));
                    }
                }
                lastDate = td.getDate();
                break;
            }
        }
        Assert.assertEquals(100, nb);
    }

    private TimedData insertData(String id, String mark, Date date) {
        Map<String, Object> map = new TreeMap<>();
        map.put("mark", mark);
        map.put("date", "" + date);
        TimedData td = new TimedData(id, null, date, map);
        TimeSerie.save(td);
        return td;
    }

    @Test
    public void searchWithinRange() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String id = "dev-" + UUID.randomUUID();
        TimedData first, last;
        first = insertData(id, "a", sdf.parse("2013-08-02 00:00:00"));
        insertData(id, "b", sdf.parse("2013-08-03 00:00:00"));
        insertData(id, "c", sdf.parse("2013-08-04 00:00:00"));
        insertData(id, "d", sdf.parse("2013-08-05 00:00:00"));
        insertData(id, "e", sdf.parse("2013-08-06 00:00:00"));
        last = insertData(id, "f", sdf.parse("2013-08-07 00:00:00"));
        { // We want to test the complete range
            ArrayList<TimedData> results = Lists.newArrayList(TimeSerie.getData(id, null, sdf.parse("2013-08-01 23:59:59"), sdf.parse("2013-08-07 00:00:01"), false));
            Assert.assertEquals(6, results.size());
        }

        { // And then between start and end (included)
            ArrayList<TimedData> results = Lists.newArrayList(TimeSerie.getData(id, null, sdf.parse("2013-08-02 00:00:00"), sdf.parse("2013-08-07 00:00:00"), false));
            Assert.assertEquals(6, results.size());
        }

        { // And then between start and end (excluded)
            ArrayList<TimedData> results = Lists.newArrayList(TimeSerie.getData(id, null, first.getDateUUID(), last.getDateUUID(), false));
            Assert.assertEquals(4, results.size());
        }

        { // And then only one
            ArrayList<TimedData> results = Lists.newArrayList(TimeSerie.getData(id, null, sdf.parse("2013-08-01 23:59:59"), sdf.parse("2013-08-02 00:00:01"), false));
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void deleteAll() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "dev-" + UUID.randomUUID();
        insertData(id, "a", sdf.parse("2013-08-02 00:00:00"));
        insertData(id, "b", sdf.parse("2013-08-03 00:00:00"));
        insertData(id, "c", sdf.parse("2013-08-04 00:00:00"));
        insertData(id, "d", sdf.parse("2013-08-05 00:00:00"));
        insertData(id, "e", sdf.parse("2013-08-06 00:00:00"));
        insertData(id, "f", sdf.parse("2013-08-07 00:00:00"));
        Assert.assertEquals(6, Lists.newArrayList(TimeSerie.getData(id)).size());

        // We delete everything (10 years back)
        TimeSerie.delete(id, null);

        Assert.assertEquals(0, Lists.newArrayList(TimeSerie.getData(id)).size());
    }

    @Test
    public void deletePreciselyHalf() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "dev-" + UUID.randomUUID();
        insertData(id, "a", sdf.parse("2013-08-02 00:00:00"));
        insertData(id, "b", sdf.parse("2013-08-03 00:00:00"));
        insertData(id, "c", sdf.parse("2013-08-04 00:00:00"));
        insertData(id, "d", sdf.parse("2013-08-05 00:00:00"));
        insertData(id, "e", sdf.parse("2013-08-06 00:00:00"));
        insertData(id, "f", sdf.parse("2013-08-07 00:00:00"));

        // We get everything and delete half of it
        for (TimedData td : TimeSerie.getData(id)) {
            String mark = (String) td.getJsonMap().get("mark");
            if (mark.charAt(0) % 2 == 0) {
                td.delete(); // Deleting while itering over results is not an issue
            }
        }
        Assert.assertEquals(Lists.newArrayList(TimeSerie.getData(id)).size(), 3);
    }

    @Test
    public void deletePreciselyManually() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "dev-" + UUID.randomUUID();
        TimedData first, last;
        insertData(id, "a", sdf.parse("2013-08-02 00:00:00"));
        insertData(id, "b", sdf.parse("2013-08-03 00:00:00"));
        first = insertData(id, "c", sdf.parse("2013-08-04 00:00:00"));
        insertData(id, "d", sdf.parse("2013-08-05 00:00:00"));
        last = insertData(id, "e", sdf.parse("2013-08-06 00:00:00"));
        insertData(id, "f", sdf.parse("2013-08-07 00:00:00"));

        // We get half of the data using the ranges, and delete it
        for (TimedData td : TimeSerie.getData(id, null, first.getDate(), last.getDate(), true)) {
            String mark = (String) td.getJsonMap().get("mark");
            // We also make sure we're fetching good results
            Assert.assertTrue(mark.equals("c") || mark.equals("d") || mark.equals("e"));
            td.delete();
        }
        Assert.assertEquals(3, Lists.newArrayList(TimeSerie.getData(id)).size());
    }

    @Test
    public void deletePreciselyAutomatically() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "dev-" + UUID.randomUUID();
        TimedData first, last;
        insertData(id, "a", sdf.parse("2013-08-02 00:00:00"));
        insertData(id, "b", sdf.parse("2013-08-03 00:00:00"));
        first = insertData(id, "c", sdf.parse("2013-08-04 00:00:00"));
        insertData(id, "d", sdf.parse("2013-08-05 00:00:00"));
        last = insertData(id, "e", sdf.parse("2013-08-06 00:00:00"));
        insertData(id, "f", sdf.parse("2013-08-07 00:00:00"));

        // We delete half of the data (using getDate and not getDateUUID is very important here)
        TimeSerie.delete(id, null, first.getDate(), last.getDate());
        Assert.assertEquals(3, Lists.newArrayList(TimeSerie.getData(id)).size());
    }

    @Test
    public void deleteRoughlyHalf() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "dev-" + UUID.randomUUID();
        TimedData first, last;
        insertData(id, "a", sdf.parse("2013-01-01 00:00:00"));
        insertData(id, "b", sdf.parse("2013-02-02 00:00:00"));
        first = insertData(id, "c", sdf.parse("2013-03-01 00:00:00"));
        insertData(id, "d", sdf.parse("2013-04-01 00:00:00"));
        last = insertData(id, "e", sdf.parse("2013-05-01 00:00:00"));
        insertData(id, "f", sdf.parse("2013-06-01 00:00:00"));

        // We delete half of the data but with a monthly precision (no read ==> much faster)
        TimeSerie.deleteRoughly(id, null, first.getDate(), last.getDate());
        Assert.assertEquals(3, Lists.newArrayList(TimeSerie.getData(id)).size());
    }

    @Test
    public void deletePreciselyByPeriod() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "dev-" + UUID.randomUUID();
        insertData(id, "a-", sdf.parse("2013-07-31 00:00:00"));
        insertData(id, "a", sdf.parse("2013-08-02 00:00:00"));
        insertData(id, "b", sdf.parse("2013-08-03 00:00:00"));
        insertData(id, "c", sdf.parse("2013-08-04 00:00:00"));
        insertData(id, "d", sdf.parse("2013-08-29 00:00:00"));
        insertData(id, "e", sdf.parse("2013-08-30 00:00:00"));
        insertData(id, "f", sdf.parse("2013-08-31 00:00:00"));
        insertData(id, "a+", sdf.parse("2013-09-01 00:00:00"));

        Assert.assertEquals(8, Lists.newArrayList(TimeSerie.getData(id)).size());

        {// We delete the data for a period
            int period = 2013 * 12 + 8;
            for (TimedData td : TimeSerie.getData(id, null, period, true)) {
                Assert.assertEquals(1, ((String) td.getJsonMap().get("mark")).length());
                td.delete();
            }
        }

        Assert.assertEquals(2, Lists.newArrayList(TimeSerie.getData(id)).size());
    }

    @Test
    public void periodTest() throws Exception {
        int period = 2013 * 12 + 9;
        Assert.assertEquals("2013-09", TimeSerie.periodToMonth(period));
    }

    @Test
    public void replaceEventTest() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "dev-" + UUID.randomUUID();
        insertData(id, "a", sdf.parse("2013-08-02 00:00:00"));
        insertData(id, "b", sdf.parse("2013-08-03 00:00:00"));
        insertData(id, "c", sdf.parse("2013-08-04 00:00:00"));
        insertData(id, "d", sdf.parse("2013-08-05 00:00:00"));
        insertData(id, "e", sdf.parse("2013-08-06 00:00:00"));
        insertData(id, "f", sdf.parse("2013-08-07 00:00:00"));

        // We will transform half of the marks by adding a 2
        for (TimedData td : TimeSerie.getData(id)) {
            String mark = (String) td.getJsonMap().get("mark");
            // We also make sure we're fetching good results
            if (mark.charAt(0) % 2 == 0) {
                Map<String, Object> map = td.getJsonMap();
                map.put("mark", mark + "2");
                td.overwrite(map);
            }
        }
        // We must still have 6 data (some of them must have changed though)
        Assert.assertEquals(6, Lists.newArrayList(TimeSerie.getData(id)).size());

        { // But for half of them, we must have a "2" next to it
            int nb = 0;
            for (TimedData td : TimeSerie.getData(id)) {
                String mark = (String) td.getJsonMap().get("mark");
                // We also make sure we're fetching good results
                if (mark.length() == 2) {
                    nb++;
                }
            }
            Assert.assertEquals(3, nb);
        }
    }

    @Test
    public void replaceEventTest2() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "dev-" + UUID.randomUUID();
        insertData(id, "a", sdf.parse("2013-08-02 00:00:00"));
        insertData(id, "b", sdf.parse("2013-08-03 00:00:00"));
        insertData(id, "c", sdf.parse("2013-08-04 00:00:00"));
        insertData(id, "d", sdf.parse("2013-08-05 00:00:00"));
        insertData(id, "e", sdf.parse("2013-08-06 00:00:00"));
        insertData(id, "f", sdf.parse("2013-08-07 00:00:00"));

        // We will transform half of the marks by adding a 2
        for (TimedDataWrapper tdw : TimedDataWrapper.iter(TimeSerie.getData(id))) {
            String mark = tdw.getString("mark");
            // We also make sure we're fetching good results
            if (mark.charAt(0) % 2 == 0) {
                tdw.set("mark", mark + "2");
                tdw.save();
            }
        }
        // We must still have 6 data (some of them must have changed though)
        Assert.assertEquals(6, Lists.newArrayList(TimeSerie.getData(id)).size());

        { // But for half of them, we must have a "2" next to it
            int nb = 0;
            for (TimedDataWrapper tdw : TimedDataWrapper.iter(TimeSerie.getData(id))) {
                String mark = tdw.getString("mark");
                // We also make sure we're fetching good results
                if (mark.length() == 2) {
                    nb++;
                }
            }
            Assert.assertEquals(3, nb);
        }
    }
}
