package org.m2mp.db.common;

import com.datastax.driver.core.TableMetadata;
import org.apache.log4j.Logger;
import org.m2mp.db.DB;

/**
 *
 * @author Florent Clairambault
 */
public class TableCreation {

	public static void checkTable(DB db, TableIncrementalDefinition tableDef) {
		int version;
		
//		GeneralSetting gs = new GeneralSetting(db);
		
		if (!tableExists(db, tableDef.getTableDefName())) {
			version = -1;
		} else {
			version = GeneralSetting.get(db, "table_version_" + tableDef.getTableDefName(), 0);
		}
//		int lastVersion = version;
		try {
			for (TableIncrementalDefinition.TableChange tc : tableDef.getTableDefChanges()) {
				if (tc.version > version) {
					System.out.println("Executing \"" + tc.cql + "\"...");
					try {
						db.execute(tc.cql);
					} catch (Exception ex) {
						Logger.getRootLogger().warn("CQL execution issue", ex);
					}
					version = tc.version;
				}
			}
		} finally {
			GeneralSetting.set(db, "table_version_" + tableDef.getTableDefName(), version);
		}
	}

	public static boolean tableExists(DB db, String tableName) {
		TableMetadata table = db.meta().getTable(tableName);
		return table != null;
	}
}
