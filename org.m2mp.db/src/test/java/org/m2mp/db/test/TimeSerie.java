/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.ts.TimeSeries;
import org.m2mp.db.ts.TimedData;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author florent
 */
public class TimeSerie {

	@BeforeClass
	public static void setUpClass() {
		BaseTest.setUpClass();
		try {
			DB.session().execute("drop table TimeSeries;");
		} catch (Exception ex) {
		}
		TimeSeries.prepareTable();
	}

	@Test
	public void insertStringNoType() {
		String id = "insert-without-type";
		TimeSeries.save(new TimedData(id, null, "{\"lat\":48.8,\"lon\":2.5,\"_type\":\"loc\"}"));
	}

	@Test
	public void insert1000Strings() throws InterruptedException {
		String id = "insert-with-type";
		Date d = new Date();
		for (int i = 0; i < 1000; i++) {
			d.setTime(d.getTime() + 100);
			TimeSeries.save(new TimedData(id, "loc", "{\"lat\":48.8,\"lon\":2.5}"));
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
		TimeSeries.save(new TimedData(id, "loc", d1, map));

		Map<String, Object> map2 = new TreeMap<>();
		map2.put("previous", map);
		map2.put("lat", 45.8);
		map2.put("lon", 1.5);
		TimeSeries.save(new TimedData(id, "loc", d2, map2));
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
			TimeSeries.save(new TimedData(id, null, d, map));
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
			for (TimedData td : TimeSeries.getData(id, null, null, null, true)) {
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
			for (TimedData td : TimeSeries.getData(id, null, null, null, false)) {
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
		System.out.println("ID: " + id);
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
		for (TimedData td2 : TimeSeries.getData(id, null, null, null, inverted)) {
			td = td2;
			nb++;
			break;
		}
//		System.out.println("First: " + td.getJson());
		Date lastDate = null;
		while (ok) {
			ok = false;
			for (TimedData td2 : TimeSeries.getData(id, null, inverted ? null : td.getDate(), inverted ? td.getDate() : null, inverted)) {
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
}
