/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import org.m2mp.db.DB;

/**
 * General settings management.
 *
 * This is a dirty K/V store.
 *
 * @author Florent Clairambault
 */
public class GeneralSetting {

	public static final String TABLE = "GeneralSettings";
	// <editor-fold defaultstate="collapsed" desc="Get value">

	/**
	 * Get a general setting value.
	 *
	 * @param name Name of the value
	 * @param defaultValue Default value
	 * @return Value
	 */
	public static int get(String name, int defaultValue) {
		String value = get(name, (String) null);
		return value != null ? Integer.parseInt(value) : defaultValue;
	}

	/**
	 * Get a general setting value.
	 *
	 * @param name Name of the value
	 * @param defaultValue Default value
	 * @return Value
	 */
	public static String get(String name, String defaultValue) {
		ResultSet rs = DB.execute(DB.prepare("SELECT value FROM " + TABLE + " WHERE name = ?;").bind(name));
		for (Row r : rs) {
			return r.getString(0);
		}
		return defaultValue;
	}
	// </editor-fold>

	// <editor-fold defaultstate="collapsed" desc="Set value">
	public static void set(String name, int value) {
		set(name, "" + value);
	}

	public static ResultSet set(String name, String value) {
		return DB.execute(DB.prepare("INSERT INTO " + TABLE + " ( name, value ) VALUES ( ?, ? );").bind(name, value));
	}

	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Table creation">
	public static void prepareTable() {
		TableCreation.checkTable(new TableIncrementalDefinition() {
			@Override
			public String getTableDefName() {
				return TABLE;
			}

			@Override
			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
				list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + getTableDefName() + " ( name text PRIMARY KEY, value text );"));
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
