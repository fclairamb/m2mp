/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import org.m2mp.db.DB;

/**
 *
 * @author Florent Clairambault
 */
public class GeneralSetting {

	private static final String TABLE_GENERAL_SETTINGS = "general_settings";

	// <editor-fold defaultstate="collapsed" desc="Get value">
	public static int get(String name, int defaultValue) {
		String value = get(name, (String) null);
		return value != null ? Integer.parseInt(value) : defaultValue;
	}

	public static String get(String name, String defaultValue) {
		ResultSet rs = DB.sess().execute(reqGet().bind(name));
		for (Row r : rs) {
			return r.getString(0);
		}
		return defaultValue;
	}

	private static PreparedStatement reqGet() {
		if (reqGet == null) {
			reqGet = DB.sess().prepare("SELECT value FROM " + TABLE_GENERAL_SETTINGS + " WHERE name = ?;");
		}
		return reqGet;
	}
	private static PreparedStatement reqGet;
	// </editor-fold>

	// <editor-fold defaultstate="collapsed" desc="Set value">
	static void set(String name, int value) {
		set(name, "" + value);
	}

	public static ResultSet set(String name, String value) {
		return DB.sess().execute(reqSet().bind(name, value));
	}

	private static PreparedStatement reqSet() {
		if (reqSet == null) {
			reqSet = DB.sess().prepare("INSERT INTO " + TABLE_GENERAL_SETTINGS + " ( name, value ) VALUES ( ?, ? );");
		}
		return reqSet;
	}
	private static PreparedStatement reqSet;
	// </editor-fold>

	// <editor-fold defaultstate="collapsed" desc="Table creation">
	public static void prepareTable() {
		TableCreation.checkTable(new TableIncrementalDefinition() {
			@Override
			public String getTableDefName() {
				return TABLE_GENERAL_SETTINGS;
			}

			@Override
			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
				list.add(new TableIncrementalDefinition.TableChange(1, "create table " + getTableDefName() + " ( name text primary key, value text );"));
				return list;
			}

			@Override
			public int getTableDefVersion() {
				return 1;
			}
		});
	}
	// </editor-fold>
}
