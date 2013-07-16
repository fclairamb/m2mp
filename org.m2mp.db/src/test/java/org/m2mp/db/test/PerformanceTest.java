/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import com.datastax.driver.core.PreparedStatement;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DBAccess;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.entity.Domain;

/**
 *
 * @author florent
 */
public class PerformanceTest {

	static DBAccess db;

	@BeforeClass
	public static void setUpClass() {
		db = DBAccess.getOrCreate("ks_test");
		try {
			db.execute("drop table Domain;");
			db.execute("drop table RegistryNode;");
			db.execute("drop table RegistryNodeChildren;");
			db.execute("drop table RegistryNodeData;");
		} catch (Exception ex) {
		}
		GeneralSetting.prepareTable(db);
		Domain.prepareTable(db);
	}

	@Test
	public void shouldBeSlow() { // 21.5s
		for (int i = 0; i < 20000; i++) {
			db.execute(db.prepareNoCache("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );").bind("name", "value"));
		}
	}

	@Test
	public void shouldBeFast() { // 8.9s
		PreparedStatement ps = db.prepareNoCache("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );");
		for (int i = 0; i < 20000; i++) {
			db.execute(ps.bind("name", "value"));
		}
	}

	@Test
	public void iWonderAboutThisOne() { // 8.9s
		for (int i = 0; i < 20000; i++) {
			db.execute(db.prepare("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );").bind("name", "value"));
		}
	}
}
