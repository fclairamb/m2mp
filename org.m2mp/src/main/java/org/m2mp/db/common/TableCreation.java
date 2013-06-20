package org.m2mp.db.common;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 *
 * @author florent
 */
public class TableCreation {

	public static void checkTable(Session session, TableIncrementalDefinition tableDef) {
		int version;
		if (!tableExists(session, tableDef.getTableDefName())) {
			version = -1;
		} else {
			version = GeneralSetting.get(session, "table_version_" + tableDef.getTableDefName(), -1);
		}
		int lastVersion = version;
		for (TableIncrementalDefinition.TableChange tc : tableDef.getTableDefChanges()) {
			if (tc.version < version) {
				session.execute(tc.cql);
				if (version > lastVersion) {
					lastVersion = version;
				}
			}
		}
		GeneralSetting.set(session, "table_version_" + tableDef.getTableDefName(), lastVersion);
	}

	public static boolean tableExists(Session session, String tableName) {
		TableMetadata table = session.getCluster().getMetadata().getKeyspace("ks").getTable(tableName);
		return table != null;
	}
}
