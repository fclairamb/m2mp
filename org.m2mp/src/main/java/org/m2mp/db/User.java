/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db;

import java.util.ArrayList;
import java.util.List;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author florent
 */
public class User {

	private static final String TABLE_USER = "User";
	public static final TableIncrementalDefinition DEFINITION = new TableIncrementalDefinition() {
		public String getTableDefName() {
			return TABLE_USER;
		}

		public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
			List<TableIncrementalDefinition.TableChange> list = new ArrayList<TableIncrementalDefinition.TableChange>();
			list.add(new TableIncrementalDefinition.TableChange(1, ""
					+ "CREATE TABLE " + TABLE_USER + " (\n"
					+ "  name text PRIMARY KEY,"
					+ "  domain uuid\n"
					+ ");"));
			return list;
		}

		public String getTablesDefCql() {
			return ""
					+ "CREATE TABLE " + TABLE_USER + " (\n"
					+ "  name text PRIMARY KEY,"
					+ "  domain uuid\n"
					+ ");";
		}

		public int getTableDefVersion() {
			return 1;
		}
	};
}
