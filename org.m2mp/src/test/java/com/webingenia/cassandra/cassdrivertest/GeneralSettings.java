/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.webingenia.cassandra.cassdrivertest;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.logging.Logger;
import junit.framework.Assert;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.common.TableCreation;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.Shared;

/**
 *
 * @author florent
 */
public class GeneralSettings {

	@BeforeClass
	public static void setUpClass() {
		BaseTest.setUpClass();
		try {
			Shared.db().execute("drop table general_settings;");
		} catch (Exception ex) {
		}
		TableCreation.checkTable(GeneralSetting.DEFINITION);
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
