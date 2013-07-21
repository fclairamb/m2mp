/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import com.google.common.collect.Lists;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.ts.TimeSerie;
import org.m2mp.db.ts.TimedData;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.registry.RegistryNode;

/**
 *
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
}
