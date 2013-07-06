/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import java.util.Date;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.Shared;
import org.m2mp.db.TimeSeries;
import org.m2mp.db.TimedData;
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
			Shared.db().execute("drop table TimeSeries;");
		} catch (Exception ex) {
		}
		TimeSeries.prepareTable();
	}

	@Test
	public void insert() throws InterruptedException {
		String id = UUID.randomUUID().toString();
		Date d = new Date();
		for (int i = 0; i < 10000; i++) {
			d.setTime(d.getTime() + 100);
			TimeSeries.save(new TimedData(id, "loc", "{\"lat\":48.8,\"lon\":2.5}"));
		}
	}
}
