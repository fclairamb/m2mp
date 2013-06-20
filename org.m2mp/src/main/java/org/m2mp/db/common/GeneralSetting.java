/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author florent
 */
public class GeneralSetting {

	public static String get(Session session, String name, String defaultValue) {
		PreparedStatement ps = session.prepare("SELECT value FROM " + TABLE_GENERAL_SETTINGS + " WHERE name = ?;");
		ps.bind(name);
		for (Row r : session.execute(ps.getQueryString())) {
			return r.getString(1);
		}
		return defaultValue;
	}

	public static int get(Session session, String name, int defaultValue) {
		String value = get(session, name, (String) null);
		return value != null ? Integer.parseInt(value) : defaultValue;
	}

	public static ResultSet set(Session session, String name, String value) {
		PreparedStatement ps = session.prepare("INSERT INTO " + TABLE_GENERAL_SETTINGS + " ( name, value ) VALUES ( ?, ? );");
		ps.bind(name, value);
		return session.execute(ps.getQueryString());
	}
	private static final String TABLE_GENERAL_SETTINGS = "general_settings";

	static void set(Session session, String name, int value) {
		set(session, name, "" + value);
	}
	public static final TableIncrementalDefinition DEFINITION = new TableIncrementalDefinition() {
		public String getTableDefName() {
			return TABLE_GENERAL_SETTINGS;
		}

		public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
			List<TableIncrementalDefinition.TableChange> list = new ArrayList<TableIncrementalDefinition.TableChange>();
			list.add(new TableIncrementalDefinition.TableChange(1, "create table " + getTableDefName() + " ( name text, value text );"));
			return list;
		}

		public int getTableDefVersion() {
			return 1;
		}
	};
}
