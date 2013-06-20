/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import java.util.List;

/**
 * Describes the changes to apply to the table.
 * @author florent
 */
public interface TableIncrementalDefinition {

	public class TableChange {

		public TableChange(int version, String cql) {
			this.version = version;
			this.cql = cql;
		}
		final int version;
		final String cql;
	}

	String getTableDefName();

	public List<TableChange> getTableDefChanges();

	public int getTableDefVersion();
}
