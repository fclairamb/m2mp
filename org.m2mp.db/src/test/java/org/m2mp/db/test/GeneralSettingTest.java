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
import org.m2mp.db.DB;

/**
 *
 * @author Florent Clairambault
 */
public class GeneralSettingTest {

	@BeforeClass
	public static void setUpClass() {
		DB.keyspace("ks_test", true);
		try {
			DB.execute("drop table " + GeneralSetting.TABLE + ";");
		} catch (Exception ex) {
		}
		GeneralSetting.prepareTable();
	}

	@Test
	public void test_save() {
		String value = GeneralSetting.get("setting-1", "value-1");
		Assert.assertEquals("value-1", value);

		GeneralSetting.set("setting-1", "value-2");
		value = GeneralSetting.get("setting-1", "value-1");
		Assert.assertEquals("value-2", value);
	}
}
