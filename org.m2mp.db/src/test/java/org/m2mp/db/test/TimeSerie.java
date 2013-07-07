/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
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
			DB.sess().execute("drop table TimeSeries;");
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
}
