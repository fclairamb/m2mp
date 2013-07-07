/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import java.util.Date;
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
	public void insertNoType() {
		String id = "insert-without-type";
		TimeSeries.save(new TimedData(id, null, "{\"lat\":48.8,\"lon\":2.5,\"_type\":\"loc\"}"));
	}

	@Test
	public void insert10000() throws InterruptedException {
		String id = "insert-with-type";
		Date d = new Date();
		for (int i = 0; i < 10000; i++) {
			d.setTime(d.getTime() + 100);
			TimeSeries.save(new TimedData(id, "loc", "{\"lat\":48.8,\"lon\":2.5}"));
		}
	}
}
