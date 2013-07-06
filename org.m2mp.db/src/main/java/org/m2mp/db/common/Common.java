/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 *
 * @author florent
 */
public class Common {

	public static final String TABLE_GENERAL_SETTINGS = "general_settings";

	public static ResultSet createGeneralSettings(Session session) {
		return session.execute("create table " + TABLE_GENERAL_SETTINGS + " ( name text primary key , version text );");
	}

	public static boolean checkGeneralSettingsExists(Session session) {
		ResultSet rs = session.execute("describe table " + TABLE_GENERAL_SETTINGS + ";");
		if (rs.all().isEmpty()) {
			return false;
		}
		return true;
	}
}
