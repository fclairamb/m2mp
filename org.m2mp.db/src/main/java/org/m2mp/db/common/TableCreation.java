package org.m2mp.db.common;

import com.datastax.driver.core.TableMetadata;
import org.apache.log4j.Logger;
import org.m2mp.db.DB;

/**
 *
 * @author Florent Clairambault
 */
public class TableCreation {

	public static void checkTable(TableIncrementalDefinition tableDef) {
		int version;
		
//		GeneralSetting gs = new GeneralSetting(db);
		
		if (!tableExists( tableDef.getTableDefName())) {
			version = -1;
		} else {
			version = GeneralSetting.get("table_version_" + tableDef.getTableDefName(), 0);
		}
//		int lastVersion = version;
		try {
			for (TableIncrementalDefinition.TableChange tc : tableDef.getTableDefChanges()) {
				if (tc.version > version) {
					System.out.println("Executing \"" + tc.cql + "\"...");
					try {
						DB.execute(tc.cql);
					} catch (Exception ex) {
						Logger.getRootLogger().warn("CQL execution issue", ex);
					}
					version = tc.version;
				}
			}
		} finally {
			GeneralSetting.set("table_version_" + tableDef.getTableDefName(), version);
		}
	}

	public static boolean tableExists(String tableName) {
		TableMetadata table = DB.meta().getTable(tableName.toLowerCase());
		return table != null;
	}
}
