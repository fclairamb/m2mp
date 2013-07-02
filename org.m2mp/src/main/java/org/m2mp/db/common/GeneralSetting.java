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
import org.m2mp.db.Shared;

/**
 *
 * @author florent
 */
public class GeneralSetting {

	private static final String TABLE_GENERAL_SETTINGS = "general_settings";
	private static PreparedStatement _reqGet;

	private static PreparedStatement reqGet() {
		if (_reqGet == null) {
			_reqGet = Shared.db().prepare("SELECT value FROM " + TABLE_GENERAL_SETTINGS + " WHERE name = ?;");
		}
		return _reqGet;
	}
	private static PreparedStatement _reqSet;

	private static PreparedStatement reqSet() {
		if (_reqSet == null) {
			_reqSet = Shared.db().prepare("INSERT INTO " + TABLE_GENERAL_SETTINGS + " ( name, value ) VALUES ( ?, ? );");
		}
		return _reqSet;
	}

	public static String get(String name, String defaultValue) {
		ResultSet rs = Shared.db().execute(reqGet().bind(name));
		for (Row r : rs) {
			return r.getString(0);
		}
		return defaultValue;
	}

	public static int get(String name, int defaultValue) {
		String value = get(name, (String) null);
		return value != null ? Integer.parseInt(value) : defaultValue;
	}

	public static ResultSet set(String name, String value) {
		return Shared.db().execute(reqSet().bind(name, value));
	}

	static void set(String name, int value) {
		set(name, "" + value);
	}
	public static final TableIncrementalDefinition DEFINITION = new TableIncrementalDefinition() {
		public String getTableDefName() {
			return TABLE_GENERAL_SETTINGS;
		}

		public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
			List<TableIncrementalDefinition.TableChange> list = new ArrayList<TableIncrementalDefinition.TableChange>();
			list.add(new TableIncrementalDefinition.TableChange(1, "create table " + getTableDefName() + " ( name text primary key, value text );"));
			return list;
		}

		public String getTablesDefCql() {
			return "create table " + getTableDefName() + " ( name text primary key, value text );";
		}

		public int getTableDefVersion() {
			return 1;
		}
	};
}
