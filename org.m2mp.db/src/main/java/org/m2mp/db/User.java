/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db;

import java.util.ArrayList;
import java.util.List;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author Florent Clairambault
 */
public class User extends Entity {

	private static final String TABLE_USER = "User";
	public static final TableIncrementalDefinition DEFINITION = new TableIncrementalDefinition() {
		@Override
		public String getTableDefName() {
			return TABLE_USER;
		}

		public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
			List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
			list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_USER + " name text PRIMARY KEY, id uuid, domain uuid );"));
			return list;
		}

		public String getTablesDefCql() {
			return "CREATE TABLE " + TABLE_USER + " name text PRIMARY KEY, id uuid, domain uuid );";
		}

		@Override
		public int getTableDefVersion() {
			return 1;
		}
	};
}
