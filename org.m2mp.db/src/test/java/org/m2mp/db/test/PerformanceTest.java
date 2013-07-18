/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import com.datastax.driver.core.PreparedStatement;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.entity.Domain;

/**
 *
 * @author florent
 */
public class PerformanceTest {

//	static DB db;
	@BeforeClass
	public static void setUpClass() {
		DB.keyspace("ks_test", true);
		try {
			DB.execute("drop table Domain;");
			DB.execute("drop table RegistryNode;");
			DB.execute("drop table RegistryNodeChildren;");
			DB.execute("drop table RegistryNodeData;");
		} catch (Exception ex) {
		}
		GeneralSetting.prepareTable();
		Domain.prepareTable();
	}

	@Test
	public void shouldBeSlow() { // 21.5s
		for (int i = 0; i < 20000; i++) {
			DB.execute(DB.prepareNoCache("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );").bind("name", "value"));
		}
	}

	@Test
	public void shouldBeFast() { // 8.9s
		PreparedStatement ps = DB.prepareNoCache("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );");
		for (int i = 0; i < 20000; i++) {
			DB.execute(ps.bind("name", "value"));
		}
	}

	@Test
	public void iWonderAboutThisOne() { // 8.9s
		for (int i = 0; i < 20000; i++) {
			DB.execute(DB.prepare("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );").bind("name", "value"));
		}
	}
}
