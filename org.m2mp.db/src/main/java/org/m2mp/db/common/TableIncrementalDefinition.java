/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import java.util.List;

/**
 * Describes the changes to apply to the table.
 *
 * @author Florent Clairambault
 */
public interface TableIncrementalDefinition {

	public class TableChange {

		public TableChange(int version, String cql) {
			this.version = version;
			this.cql = cql;
		}
		public final int version;
		public final String cql;
	}

	/**
	 * Name of the table
	 */
	String getTableDefName();

	/**
	 * Succession of changes applied to this table
	 */
	List<TableChange> getTableDefChanges();

	/**
	 * Current version of the table
	 */
	int getTableDefVersion();
}
