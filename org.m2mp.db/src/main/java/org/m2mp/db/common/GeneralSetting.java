/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import org.m2mp.db.DBAccess;

/**
 *
 * @author Florent Clairambault
 */
public class GeneralSetting {

	public static final String TABLE = "generalsettings";
	// <editor-fold defaultstate="collapsed" desc="Get value">

	public static int get(DBAccess db, String name, int defaultValue) {
		String value = get(db, name, (String) null);
		return value != null ? Integer.parseInt(value) : defaultValue;
	}

	public static String get(DBAccess db, String name, String defaultValue) {
		ResultSet rs = db.execute(db.prepare("SELECT value FROM " + TABLE + " WHERE name = ?;").bind(name));
		for (Row r : rs) {
			return r.getString(0);
		}
		return defaultValue;
	}
	// </editor-fold>

	// <editor-fold defaultstate="collapsed" desc="Set value">
	public static void set(DBAccess db, String name, int value) {
		set(db, name, "" + value);
	}

	public static ResultSet set(DBAccess db, String name, String value) {
		return db.execute(db.prepare("INSERT INTO " + TABLE + " ( name, value ) VALUES ( ?, ? );").bind(name, value));
	}

	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Table creation">
	public static void prepareTable(DBAccess db) {
		TableCreation.checkTable(db, new TableIncrementalDefinition() {
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
