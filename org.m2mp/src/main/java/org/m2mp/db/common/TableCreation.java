package org.m2mp.db.common;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import org.m2mp.db.Shared;

/**
 *
 * @author florent
 */
public class TableCreation {

	public static void checkTable(TableIncrementalDefinition tableDef) {
		int version;
		if (!tableExists(tableDef.getTableDefName())) {
			version = -1;
		} else {
			version = GeneralSetting.get("table_version_" + tableDef.getTableDefName(), 0);
		}
//		int lastVersion = version;
		try {
			if (version == -1) {
				String cql = tableDef.getTablesDefCql();
				Shared.db().execute(cql);
				version = tableDef.getTableDefVersion();
			}
			for (TableIncrementalDefinition.TableChange tc : tableDef.getTableDefChanges()) {
				if (tc.version > version) {
					Shared.db().execute(tc.cql);
					version = tc.version;
				}
			}
		} finally {
			GeneralSetting.set("table_version_" + tableDef.getTableDefName(), version);
		}
	}

	public static boolean tableExists(String tableName) {
		TableMetadata table = Shared.dbMgmt().getTable(tableName);
		return table != null;
	}
}
