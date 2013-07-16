/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import junit.framework.Assert;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.common.TableCreation;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DBAccess;

/**
 *
 * @author Florent Clairambault
 */
public class GeneralSettingTest {

	static DBAccess db;

	@BeforeClass
	public static void setUpClass() {
		db = DBAccess.getOrCreate("ks_test");
		try {
			db.execute("drop table " + GeneralSetting.TABLE_GENERAL_SETTINGS + ";");
		} catch (Exception ex) {
		}
		GeneralSetting.prepareTable(db);
	}

	@Test
	public void test_save() {
		String value = GeneralSetting.get(db, "setting-1", "value-1");
		Assert.assertEquals("value-1", value);

		GeneralSetting.set(db, "setting-1", "value-2");
		value = GeneralSetting.get(db, "setting-1", "value-1");
		Assert.assertEquals("value-2", value);
	}
}
