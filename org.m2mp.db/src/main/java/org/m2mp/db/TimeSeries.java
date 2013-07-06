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
public class TimeSeries {

	private static final String TABLE_TIMESERIES = "TimeSeries";
	public static final TableIncrementalDefinition DEFINITION = new TableIncrementalDefinition() {
		@Override
		public String getTableDefName() {
			return TABLE_TIMESERIES;
		}

		@Override
		public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
			List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
			list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_TIMESERIES + " ( id varchar, year int, type text, date timeuuid, data blob, PRIMARY KEY ((id, year, type), date) ) WITH CLUSTERING ORDER BY (date DESC);"));
			return list;
		}

		public String getTablesDefCql() {
			return "CREATE TABLE " + TABLE_TIMESERIES + " ( id varchar, year int, type text, date timeuuid, data blob, PRIMARY KEY ((id, year, type), date) ) WITH CLUSTERING ORDER BY (date DESC);";
		}

		@Override
		public int getTableDefVersion() {
			return 1;
		}
	};
}
